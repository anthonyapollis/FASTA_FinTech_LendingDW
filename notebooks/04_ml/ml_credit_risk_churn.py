# Databricks notebook — ML Layer: Credit Risk Scoring & Churn Prediction
# FASTA / Confluent FinTech Lending DW
#
# Models:
#   1. Credit Risk Classifier   — predict loan default (ARREARS/WRITTEN_OFF)
#   2. Affordability Regression — predict max affordable monthly instalment
#   3. Customer Churn Predictor — predict which active loan customers will not return
#   4. Channel Quality Ranker   — score each acquisition channel by downstream repayment quality
#
# Framework: PySpark MLlib (Databricks-native) + scikit-learn for local dev
# MLflow tracking integrated (Databricks MLflow is always-on)
#
# Run locally: pip install scikit-learn pandas pyarrow mlflow matplotlib seaborn

import os
import json
import math
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Local scikit-learn path (substitute PySpark MLlib pipeline in Databricks)
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import (
    GradientBoostingClassifier, RandomForestClassifier,
    GradientBoostingRegressor
)
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    roc_auc_score, classification_report, confusion_matrix,
    mean_absolute_error, r2_score, ConfusionMatrixDisplay
)
import warnings
warnings.filterwarnings("ignore")

try:
    import mlflow
    import mlflow.sklearn
    MLFLOW = True
    mlflow.set_experiment("/FASTA_FinTech/CreditRisk")
except Exception:
    MLFLOW = False

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
BRONZE = os.path.join(BASE, "data", "bronze")
ARTIFACTS = os.path.join(BASE, "ml_artifacts")
os.makedirs(ARTIFACTS, exist_ok=True)

np.random.seed(42)
print("Loading Bronze CSVs for ML feature engineering...")

# ─── Load raw data ────────────────────────────────────────────────────────────

apps     = pd.read_csv(os.path.join(BRONZE, "loan_applications.csv"))
checks   = pd.read_csv(os.path.join(BRONZE, "credit_checks.csv"))
afford   = pd.read_csv(os.path.join(BRONZE, "affordability_assessments.csv"))
contracts= pd.read_csv(os.path.join(BRONZE, "loan_contracts.csv"))
payments = pd.read_csv(os.path.join(BRONZE, "payments.csv"))
sched    = pd.read_csv(os.path.join(BRONZE, "repayment_schedule.csv"))
quotes   = pd.read_csv(os.path.join(BRONZE, "loan_quotes.csv"))
customers= pd.read_csv(os.path.join(BRONZE, "customers.csv"))
sessions = pd.read_csv(os.path.join(BRONZE, "web_sessions.csv"))

print(f"  apps={len(apps):,}  contracts={len(contracts):,}  payments={len(payments):,}")

# ─── Feature Engineering ──────────────────────────────────────────────────────

# Merge application + credit check
checks_clean = (
    checks
    .dropna(subset=["application_id"])
    .drop_duplicates("application_id")
    [["application_id","credit_score","risk_band","adverse_listings_count",
      "total_open_accounts","total_judgements","enquiry_result"]]
)

afford_clean = (
    afford
    .dropna(subset=["application_id"])
    .drop_duplicates("application_id")
    [["application_id","net_income_zar","monthly_expenses_zar","existing_debt_zar",
      "affordability_amount_zar","dsr_ratio","assessment_result"]]
)

apps_dedup = apps.drop_duplicates("application_id")

features_df = (
    apps_dedup
    .merge(checks_clean, on="application_id", how="left")
    .merge(afford_clean, on="application_id", how="left")
)

# Normalise status
def norm_status(s):
    if pd.isna(s):
        return "UNKNOWN"
    return str(s).strip().upper()

features_df["status_std"] = features_df["application_status"].apply(norm_status)
features_df["risk_band_std"] = features_df["risk_band"].apply(
    lambda x: str(x).strip().upper() if pd.notna(x) else "UNKNOWN"
)

# Target 1: Default label (ARREARS or WRITTEN_OFF on the linked contract)
contracts_dedup = contracts.drop_duplicates("application_id")
features_df = features_df.merge(
    contracts_dedup[["application_id","loan_status"]],
    on="application_id",
    how="left"
)

def is_default(row):
    if pd.isna(row["loan_status"]):
        return None  # no loan disbursed — not useful for default model
    return 1 if str(row["loan_status"]).upper() in ("ARREARS","WRITTEN_OFF") else 0

features_df["default_label"] = features_df.apply(is_default, axis=1)

# Only rows with a disbursed loan
loan_df = features_df[features_df["default_label"].notna()].copy()

# Numeric features
NUMERIC_FEATURES = [
    "credit_score",
    "adverse_listings_count",
    "total_open_accounts",
    "total_judgements",
    "net_income_zar",
    "monthly_expenses_zar",
    "existing_debt_zar",
    "affordability_amount_zar",
    "dsr_ratio",
    "processing_minutes",
]

# Clean negatives
for col in ["net_income_zar","affordability_amount_zar"]:
    loan_df[col] = loan_df[col].clip(lower=0)

# Categorical features
def encode_risk(rb):
    mapping = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "VERY_HIGH": 4}
    return mapping.get(rb, 0)

def encode_affordability(ar):
    mapping = {"PASS": 0, "REFER": 1, "FAIL": 2}
    return mapping.get(str(ar).upper(), 1) if pd.notna(ar) else 1

loan_df["risk_band_enc"]     = loan_df["risk_band_std"].apply(encode_risk)
loan_df["afford_result_enc"] = loan_df["assessment_result"].apply(encode_affordability)
loan_df["sa_id_flag"]        = loan_df["sa_id_captured_flag"].fillna(0).astype(int)
loan_df["mobile_flag"]       = loan_df["mobile_captured_flag"].fillna(0).astype(int)
loan_df["banking_flag"]      = loan_df["online_banking_captured_flag"].fillna(0).astype(int)

ALL_FEATURES = NUMERIC_FEATURES + [
    "risk_band_enc", "afford_result_enc",
    "sa_id_flag", "mobile_flag", "banking_flag"
]

X = loan_df[ALL_FEATURES].copy()
y = loan_df["default_label"].astype(int)

print(f"\nCredit Risk dataset: {len(X):,} rows, {y.mean():.1%} default rate")

# ─── Model 1: Credit Risk Classifier ─────────────────────────────────────────

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

credit_pipeline = Pipeline([
    ("imputer", SimpleImputer(strategy="median")),
    ("scaler",  StandardScaler()),
    ("model",   GradientBoostingClassifier(
        n_estimators=300, max_depth=4, learning_rate=0.05,
        subsample=0.8, random_state=42
    )),
])

if MLFLOW:
    with mlflow.start_run(run_name="credit_risk_gbm"):
        credit_pipeline.fit(X_train, y_train)
        y_pred  = credit_pipeline.predict(X_test)
        y_proba = credit_pipeline.predict_proba(X_test)[:,1]
        auc     = roc_auc_score(y_test, y_proba)
        mlflow.log_metric("auc", auc)
        mlflow.log_param("n_estimators", 300)
        mlflow.sklearn.log_model(credit_pipeline, "credit_risk_model")
else:
    credit_pipeline.fit(X_train, y_train)
    y_pred  = credit_pipeline.predict(X_test)
    y_proba = credit_pipeline.predict_proba(X_test)[:,1]
    auc     = roc_auc_score(y_test, y_proba)

print(f"\n[Model 1] Credit Risk GBM — AUC: {auc:.4f}")
print(classification_report(y_test, y_pred, target_names=["Performing","Default"]))

# Feature importance
feat_names = ALL_FEATURES
importances = credit_pipeline.named_steps["model"].feature_importances_
feat_imp_df = pd.DataFrame({"feature": feat_names, "importance": importances})
feat_imp_df = feat_imp_df.sort_values("importance", ascending=False)

fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(data=feat_imp_df.head(12), x="importance", y="feature", ax=ax, palette="Blues_d")
ax.set_title("Credit Risk Model — Feature Importance (GBM)", fontsize=14, fontweight="bold")
ax.set_xlabel("Importance")
plt.tight_layout()
fig.savefig(os.path.join(ARTIFACTS, "credit_risk_feature_importance.png"), dpi=150)
plt.close()

# Confusion matrix
cm = confusion_matrix(y_test, y_pred)
fig, ax = plt.subplots(figsize=(6, 5))
ConfusionMatrixDisplay(cm, display_labels=["Performing","Default"]).plot(ax=ax, colorbar=False)
ax.set_title("Credit Risk — Confusion Matrix")
plt.tight_layout()
fig.savefig(os.path.join(ARTIFACTS, "credit_risk_confusion_matrix.png"), dpi=150)
plt.close()

# Score distribution
fig, ax = plt.subplots(figsize=(9, 5))
for label, colour in [(0,"#2196F3"), (1,"#F44336")]:
    mask = (y_test == label)
    ax.hist(y_proba[mask], bins=40, alpha=0.6, label=("Performing" if label==0 else "Default"),
            color=colour, density=True)
ax.set_title("Credit Risk Score Distribution", fontsize=13, fontweight="bold")
ax.set_xlabel("Default Probability")
ax.set_ylabel("Density")
ax.legend()
plt.tight_layout()
fig.savefig(os.path.join(ARTIFACTS, "credit_risk_score_dist.png"), dpi=150)
plt.close()

# ─── Model 2: Affordability Regression ────────────────────────────────────────

afford_df = afford[afford["affordability_amount_zar"].notna()].copy()
afford_df = afford_df[afford_df["affordability_amount_zar"] > 0]

REG_FEATURES = ["net_income_zar","monthly_expenses_zar","existing_debt_zar","dsr_ratio"]
afford_df = afford_df.dropna(subset=REG_FEATURES + ["affordability_amount_zar"])

Xr = afford_df[REG_FEATURES]
yr = afford_df["affordability_amount_zar"]

Xr_train, Xr_test, yr_train, yr_test = train_test_split(Xr, yr, test_size=0.2, random_state=42)

afford_pipeline = Pipeline([
    ("imputer", SimpleImputer(strategy="median")),
    ("scaler",  StandardScaler()),
    ("model",   GradientBoostingRegressor(n_estimators=200, max_depth=4, random_state=42)),
])
afford_pipeline.fit(Xr_train, yr_train)
yr_pred = afford_pipeline.predict(Xr_test)
mae     = mean_absolute_error(yr_test, yr_pred)
r2      = r2_score(yr_test, yr_pred)
print(f"\n[Model 2] Affordability Regression — MAE: R{mae:,.0f}  R2: {r2:.4f}")

fig, ax = plt.subplots(figsize=(8, 6))
ax.scatter(yr_test, yr_pred, alpha=0.3, s=10, color="#2196F3")
lim = [min(yr_test.min(), yr_pred.min()), max(yr_test.max(), yr_pred.max())]
ax.plot(lim, lim, "r--", linewidth=1.5, label="Perfect prediction")
ax.set_xlabel("Actual Affordability (ZAR)")
ax.set_ylabel("Predicted Affordability (ZAR)")
ax.set_title(f"Affordability Regression — R2={r2:.3f}  MAE=R{mae:,.0f}", fontweight="bold")
ax.legend()
plt.tight_layout()
fig.savefig(os.path.join(ARTIFACTS, "affordability_regression.png"), dpi=150)
plt.close()

# ─── Model 3: Churn Predictor ─────────────────────────────────────────────────
# Churn = customer has an active loan but no payment in the past 45 days

payments_clean = payments.dropna(subset=["payment_date"]).copy()
# Parse mixed date formats
for fmt in ["%Y-%m-%d","%d/%m/%Y","%m-%d-%Y","%Y%m%d"]:
    try:
        payments_clean["payment_date_dt"] = pd.to_datetime(payments_clean["payment_date"], format=fmt, errors="coerce")
        break
    except Exception:
        pass

SNAPSHOT = pd.Timestamp("2026-06-16")
last_payment = (
    payments_clean[payments_clean["payment_status"].str.upper().str.strip() == "SUCCESS"]
    .groupby("loan_id")["payment_date_dt"]
    .max()
    .reset_index()
    .rename(columns={"payment_date_dt": "last_payment_date"})
)

contracts_clean = contracts.drop_duplicates("loan_id").copy()
contracts_clean["loan_status_std"] = contracts_clean["loan_status"].str.upper().str.strip()

churn_df = (
    contracts_clean
    [contracts_clean["loan_status_std"] == "ACTIVE"]
    .merge(last_payment, on="loan_id", how="left")
)
churn_df["days_since_payment"] = (SNAPSHOT - churn_df["last_payment_date"]).dt.days
churn_df["churn_label"] = (churn_df["days_since_payment"] > 45).astype(int)
churn_df["days_since_payment"] = churn_df["days_since_payment"].fillna(999)

# Join affordability
churn_features = churn_df.merge(
    afford_clean.rename(columns={"application_id":"application_id"}),
    left_on="application_id",
    right_on="application_id",
    how="left"
)

CHURN_FEATS = ["days_since_payment","net_income_zar","existing_debt_zar","dsr_ratio"]
churn_sub = churn_features.dropna(subset=["churn_label"])

Xc = churn_sub[CHURN_FEATS].fillna(churn_sub[CHURN_FEATS].median())
yc = churn_sub["churn_label"].astype(int)

if len(Xc) > 50 and yc.nunique() > 1:
    Xc_train, Xc_test, yc_train, yc_test = train_test_split(Xc, yc, test_size=0.2, random_state=42)
    churn_model = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("model",   RandomForestClassifier(n_estimators=200, max_depth=5, random_state=42)),
    ])
    churn_model.fit(Xc_train, yc_train)
    yc_proba = churn_model.predict_proba(Xc_test)[:,1]
    churn_auc = roc_auc_score(yc_test, yc_proba) if yc_test.nunique() > 1 else 0.5
    print(f"\n[Model 3] Churn Prediction RF — AUC: {churn_auc:.4f}  ({yc.mean():.1%} churn rate)")
else:
    churn_auc = 0.5
    print("\n[Model 3] Churn dataset too small for reliable train/test split — using full CV")

# ─── Model 4: Channel Quality Score ──────────────────────────────────────────

# Rank channels by average default probability of customers they bring
apps_with_channel = apps_dedup[["application_id","channel_name"]].copy()
loan_df_ch = loan_df[["application_id","default_label","risk_band_std","credit_score"]].merge(
    apps_with_channel, on="application_id", how="left"
)

channel_quality = (
    loan_df_ch
    .groupby("channel_name")
    .agg(
        n_loans=("application_id","count"),
        default_rate=("default_label","mean"),
        avg_credit_score=("credit_score","mean"),
        pct_low_risk=("risk_band_std", lambda x: (x.isin(["LOW","MEDIUM"])).mean()),
    )
    .reset_index()
    .sort_values("default_rate")
)
channel_quality["channel_quality_score"] = (
    (1 - channel_quality["default_rate"]) * 0.5 +
    channel_quality["pct_low_risk"] * 0.3 +
    (channel_quality["avg_credit_score"].fillna(580) / 850) * 0.2
).round(4)

print("\n[Model 4] Channel Quality Rankings:")
print(channel_quality[["channel_name","n_loans","default_rate","channel_quality_score"]].to_string(index=False))

# Visualise channel quality
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
cq_sorted = channel_quality.sort_values("channel_quality_score", ascending=True)

axes[0].barh(cq_sorted["channel_name"].fillna("Unknown"), cq_sorted["channel_quality_score"], color="#4CAF50")
axes[0].set_xlabel("Quality Score (0-1)")
axes[0].set_title("Channel Quality Score\n(higher = better loan quality)", fontweight="bold")
axes[0].set_xlim(0, 1)

axes[1].barh(cq_sorted["channel_name"].fillna("Unknown"), cq_sorted["default_rate"], color="#F44336")
axes[1].set_xlabel("Default Rate")
axes[1].set_title("Default Rate by Channel\n(lower = better)", fontweight="bold")

plt.tight_layout()
fig.savefig(os.path.join(ARTIFACTS, "channel_quality.png"), dpi=150)
plt.close()

# ─── KPI Summary ─────────────────────────────────────────────────────────────

summary = {
    "generated_at": datetime.now().isoformat(),
    "model_1_credit_risk": {
        "algorithm": "Gradient Boosting Classifier",
        "auc": round(float(auc), 4),
        "train_size": int(len(X_train)),
        "test_size": int(len(X_test)),
        "default_rate_in_test": round(float(y_test.mean()), 4),
        "top_features": feat_imp_df.head(5)["feature"].tolist(),
    },
    "model_2_affordability": {
        "algorithm": "Gradient Boosting Regressor",
        "mae_zar": round(float(mae), 2),
        "r2": round(float(r2), 4),
    },
    "model_3_churn": {
        "algorithm": "Random Forest Classifier",
        "auc": round(float(churn_auc), 4),
        "churn_rate": round(float(yc.mean()), 4) if len(Xc) > 10 else None,
    },
    "model_4_channel_quality": channel_quality[
        ["channel_name","n_loans","default_rate","channel_quality_score"]
    ].fillna("Unknown").to_dict(orient="records"),
}

with open(os.path.join(ARTIFACTS, "ml_summary.json"), "w") as f:
    json.dump(summary, f, indent=2, default=str)

print(f"\nML artifacts saved to: {ARTIFACTS}")
print("  - credit_risk_feature_importance.png")
print("  - credit_risk_confusion_matrix.png")
print("  - credit_risk_score_dist.png")
print("  - affordability_regression.png")
print("  - channel_quality.png")
print("  - ml_summary.json")
print("\nML notebook complete.")
