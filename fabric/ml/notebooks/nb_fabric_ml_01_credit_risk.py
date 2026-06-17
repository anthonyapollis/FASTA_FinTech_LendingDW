# =============================================================================
# Microsoft Fabric ML Notebook — nb_fabric_ml_01_credit_risk
# FASTA / Confluent FinTech Lending DW
# =============================================================================
# Purpose  : Train and register a credit default risk classifier
# Fabric   : Runs on Fabric Spark (Spark 3.4 / Runtime 1.2)
#            Uses SynapseML LightGBM + MLflow (auto-tracked in Fabric)
# Local    : Falls back to scikit-learn GBM when SynapseML unavailable
# Output   : Scored table  -> gold.fact_loan_application_scored
#            MLflow model  -> FASTA/models/credit_risk_v{version}
#            Metrics JSON  -> ml_artifacts/credit_risk_metrics.json
# =============================================================================
# Fabric notebook cells are separated by # COMMAND ----------

# COMMAND ----------
# %md
# ## Cell 1 — Environment Detection & Imports

import os, sys, json, warnings
from datetime import datetime

warnings.filterwarnings("ignore")

# Detect runtime
FABRIC = "FABRIC_WORKSPACE_ID" in os.environ or "synapse" in sys.version.lower()
LOCAL  = not FABRIC

if FABRIC:
    # ── Fabric / SynapseML path ──────────────────────────────────────────────
    from pyspark.sql import SparkSession, functions as F, types as T
    from pyspark.ml import Pipeline as SparkPipeline
    from pyspark.ml.feature import VectorAssembler, StringIndexer, Imputer
    from synapse.ml.lightgbm import LightGBMClassifier
    from synapse.ml.train import ComputeModelStatistics
    import mlflow, mlflow.spark
    spark = SparkSession.builder.appName("fasta_credit_risk").getOrCreate()
    print("Runtime: Microsoft Fabric Spark")
else:
    # ── Local scikit-learn path ──────────────────────────────────────────────
    import numpy as np
    import pandas as pd
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns
    from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
    from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.calibration import CalibratedClassifierCV, calibration_curve
    from sklearn.metrics import (
        roc_auc_score, average_precision_score, classification_report,
        confusion_matrix, ConfusionMatrixDisplay, RocCurveDisplay,
        PrecisionRecallDisplay, brier_score_loss
    )
    try:
        import mlflow, mlflow.sklearn
        mlflow.set_tracking_uri("file:./mlruns")
        mlflow.set_experiment("FASTA_CreditRisk")
        MLFLOW = True
    except ImportError:
        MLFLOW = False
    print("Runtime: Local scikit-learn (Fabric simulation)")

BASE      = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
BRONZE    = os.path.join(BASE, "data", "bronze")
ARTIFACTS = os.path.join(BASE, "fabric", "ml", "artifacts")
os.makedirs(ARTIFACTS, exist_ok=True)

# COMMAND ----------
# %md
# ## Cell 2 — Load & Merge Feature Tables

if LOCAL:
    apps      = pd.read_csv(os.path.join(BRONZE, "loan_applications.csv"))
    checks    = pd.read_csv(os.path.join(BRONZE, "credit_checks.csv"))
    afford    = pd.read_csv(os.path.join(BRONZE, "affordability_assessments.csv"))
    contracts = pd.read_csv(os.path.join(BRONZE, "loan_contracts.csv"))
    quotes    = pd.read_csv(os.path.join(BRONZE, "loan_quotes.csv"))
    customers = pd.read_csv(os.path.join(BRONZE, "customers.csv"))
    sessions  = pd.read_csv(os.path.join(BRONZE, "web_sessions.csv"))
    payments  = pd.read_csv(os.path.join(BRONZE, "payments.csv"))
    sched     = pd.read_csv(os.path.join(BRONZE, "repayment_schedule.csv"))

    # Deduplicate each table
    apps      = apps.drop_duplicates("application_id")
    checks    = checks.drop_duplicates("application_id")
    afford    = afford.drop_duplicates("application_id")
    contracts = contracts.drop_duplicates(["application_id"])
    quotes    = quotes.drop_duplicates("quote_id")
    customers = customers.drop_duplicates("customer_id")

    print(f"Loaded: apps={len(apps):,}  checks={len(checks):,}  afford={len(afford):,}  contracts={len(contracts):,}")

else:
    # Fabric — read from Silver Lakehouse
    apps      = spark.read.format("delta").load("Tables/silver/loan_applications").toPandas()
    checks    = spark.read.format("delta").load("Tables/silver/credit_checks").toPandas()
    afford    = spark.read.format("delta").load("Tables/silver/affordability_assessments").toPandas()
    contracts = spark.read.format("delta").load("Tables/silver/loan_contracts").toPandas()
    customers = spark.read.format("delta").load("Tables/silver/customers").toPandas()
    sessions  = spark.read.format("delta").load("Tables/silver/web_sessions").toPandas()

# COMMAND ----------
# %md
# ## Cell 3 — Feature Engineering

def norm_upper(s):
    return str(s).strip().upper() if pd.notna(s) else "UNKNOWN"

# ── Credit check features ────────────────────────────────────────────────────
cc = checks[["application_id","credit_score","risk_band","adverse_listings_count",
             "total_open_accounts","total_judgements","enquiry_result"]].copy()
cc["risk_band_std"]    = cc["risk_band"].apply(norm_upper)
cc["enquiry_std"]      = cc["enquiry_result"].apply(norm_upper)
cc["risk_band_enc"]    = cc["risk_band_std"].map({"LOW":1,"MEDIUM":2,"HIGH":3,"VERY_HIGH":4}).fillna(0).astype(int)
cc["credit_check_pass"]= (cc["enquiry_std"] == "PASS").astype(int)
cc["credit_score"]     = cc["credit_score"].clip(300, 850)

# ── Affordability features ───────────────────────────────────────────────────
af = afford[["application_id","net_income_zar","monthly_expenses_zar",
             "existing_debt_zar","affordability_amount_zar","dsr_ratio","assessment_result"]].copy()
af["afford_pass"]    = (af["assessment_result"].apply(norm_upper) == "PASS").astype(int)
af["net_income_zar"] = af["net_income_zar"].clip(lower=0)
af["afford_zar"]     = af["affordability_amount_zar"].clip(lower=0)
af["dsr_ratio"]      = af["dsr_ratio"].clip(0, 1.5)
# Income-to-loan ratio (added after joining quotes)
af["income_band"] = pd.cut(
    af["net_income_zar"].fillna(0),
    bins=[0,8000,15000,25000,40000,999999],
    labels=["<8K","8K-15K","15K-25K","25K-40K","40K+"]
).astype(str)

# ── Application features ─────────────────────────────────────────────────────
app_f = apps[["application_id","customer_id","processing_minutes",
               "sa_id_captured_flag","mobile_captured_flag",
               "online_banking_captured_flag","channel_name"]].copy()
app_f["docs_complete"]   = (
    app_f["sa_id_captured_flag"].fillna(0).astype(int) &
    app_f["mobile_captured_flag"].fillna(0).astype(int) &
    app_f["online_banking_captured_flag"].fillna(0).astype(int)
)
app_f["channel_enc"] = app_f["channel_name"].apply(norm_upper).map({
    "GOOGLE SPONSORED SEARCH":0,"ORGANIC SEARCH":1,"DIRECT":2,
    "EMAIL":3,"FACEBOOK":4,"WHATSAPP":5
}).fillna(6).astype(int)

# ── Contract target: default = ARREARS or WRITTEN_OFF ───────────────────────
ct = contracts[["application_id","loan_status","principal_amount_zar"]].copy()
ct["loan_status_std"] = ct["loan_status"].apply(norm_upper)
ct["default_label"]   = ct["loan_status_std"].isin(["ARREARS","WRITTEN_OFF"]).astype(int)

# ── Merge all ────────────────────────────────────────────────────────────────
df = (apps[["application_id","customer_id"]]
      .merge(cc,    on="application_id", how="inner")
      .merge(af,    on="application_id", how="left")
      .merge(app_f, on=["application_id","customer_id"], how="left")
      .merge(ct,    on="application_id", how="inner"))

df = df.dropna(subset=["default_label"])
print(f"\nModel dataset: {len(df):,} rows  |  Default rate: {df['default_label'].mean():.1%}")
print(f"Class balance: {df['default_label'].value_counts().to_dict()}")

# COMMAND ----------
# %md
# ## Cell 4 — Feature Matrix

FEATURES = [
    "credit_score",           # Credit bureau score
    "adverse_listings_count", # Number of adverse credit listings
    "total_open_accounts",    # Active credit accounts
    "total_judgements",       # Court judgements
    "risk_band_enc",          # Encoded risk band (1=LOW, 4=VERY_HIGH)
    "credit_check_pass",      # Binary: did credit check pass?
    "net_income_zar",         # Monthly net income
    "monthly_expenses_zar",   # Fixed monthly expenses
    "existing_debt_zar",      # Total existing debt
    "afford_zar",             # Assessed affordability
    "dsr_ratio",              # Debt service ratio
    "afford_pass",            # Binary: did affordability pass?
    "docs_complete",          # Binary: all 3 documents captured?
    "processing_minutes",     # Time to submit application
    "channel_enc",            # Acquisition channel
    "principal_amount_zar",   # Requested loan amount
]

X = df[FEATURES].copy()
y = df["default_label"].astype(int)

# Clip remaining negatives
for col in ["net_income_zar","monthly_expenses_zar","existing_debt_zar","afford_zar","principal_amount_zar"]:
    if col in X.columns:
        X[col] = X[col].clip(lower=0)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.20, random_state=42, stratify=y
)
print(f"Train: {len(X_train):,}  Test: {len(X_test):,}")

# COMMAND ----------
# %md
# ## Cell 5 — Train Three Models + Cross-Validation

models = {
    "GBM": Pipeline([
        ("imp",   SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
        ("clf",   GradientBoostingClassifier(
            n_estimators=400, max_depth=4, learning_rate=0.04,
            subsample=0.80, min_samples_leaf=10, random_state=42
        ))
    ]),
    "Random Forest": Pipeline([
        ("imp",   SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
        ("clf",   RandomForestClassifier(
            n_estimators=300, max_depth=6, min_samples_leaf=8,
            class_weight="balanced", random_state=42, n_jobs=-1
        ))
    ]),
    "Logistic Regression": Pipeline([
        ("imp",   SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
        ("clf",   LogisticRegression(
            C=0.5, class_weight="balanced", max_iter=500, random_state=42
        ))
    ]),
}

cv  = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
results = {}

print("\n=== Cross-Validation Results ===")
for name, pipe in models.items():
    cv_auc = cross_val_score(pipe, X_train, y_train, cv=cv, scoring="roc_auc", n_jobs=-1)
    pipe.fit(X_train, y_train)
    y_prob = pipe.predict_proba(X_test)[:,1]
    y_pred = pipe.predict(X_test)
    auc    = roc_auc_score(y_test, y_prob)
    ap     = average_precision_score(y_test, y_prob)
    brier  = brier_score_loss(y_test, y_prob)
    results[name] = {
        "pipeline": pipe, "y_prob": y_prob, "y_pred": y_pred,
        "auc": auc, "ap": ap, "brier": brier, "cv_auc_mean": cv_auc.mean(),
        "cv_auc_std": cv_auc.std()
    }
    print(f"  {name:<22} CV-AUC={cv_auc.mean():.4f}±{cv_auc.std():.4f}  "
          f"Test-AUC={auc:.4f}  AP={ap:.4f}  Brier={brier:.4f}")

best_name = max(results, key=lambda k: results[k]["auc"])
best      = results[best_name]
print(f"\nBest model: {best_name}  (AUC={best['auc']:.4f})")

# COMMAND ----------
# %md
# ## Cell 6 — MLflow Registration (Fabric: Unity Catalog; Local: file store)

if MLFLOW:
    with mlflow.start_run(run_name=f"credit_risk_{best_name.replace(' ','_').lower()}"):
        mlflow.log_param("model_type",    best_name)
        mlflow.log_param("n_train",       len(X_train))
        mlflow.log_param("n_features",    len(FEATURES))
        mlflow.log_param("default_rate",  round(float(y.mean()), 4))
        mlflow.log_metric("auc",          round(best["auc"], 4))
        mlflow.log_metric("avg_precision",round(best["ap"],  4))
        mlflow.log_metric("brier_score",  round(best["brier"],4))
        mlflow.log_metric("cv_auc_mean",  round(best["cv_auc_mean"], 4))
        mlflow.log_metric("cv_auc_std",   round(best["cv_auc_std"],  4))
        mlflow.sklearn.log_model(
            best["pipeline"],
            artifact_path="credit_risk_model",
            registered_model_name="fasta.models.credit_risk"
        )
        run_id = mlflow.active_run().info.run_id
    print(f"Logged to MLflow run: {run_id}")
else:
    run_id = "local-no-mlflow"

# COMMAND ----------
# %md
# ## Cell 7 — Risk Decile Table (Scorecard)

df_test = X_test.copy()
df_test["default_label"]   = y_test.values
df_test["default_prob"]    = best["y_prob"]
df_test["risk_decile"]     = pd.qcut(df_test["default_prob"], q=10,
                                      labels=[f"D{i}" for i in range(1, 11)])
df_test["risk_band_output"] = pd.cut(
    df_test["default_prob"],
    bins=[-0.01, 0.10, 0.20, 0.35, 0.55, 1.01],
    labels=["VERY_LOW","LOW","MEDIUM","HIGH","VERY_HIGH"]
)

decile_table = (
    df_test.groupby("risk_decile", observed=True)
    .agg(
        n=("default_label","count"),
        defaults=("default_label","sum"),
        avg_prob=("default_prob","mean"),
        min_prob=("default_prob","min"),
        max_prob=("default_prob","max"),
    )
    .assign(
        default_rate=lambda d: d["defaults"] / d["n"],
        cum_defaults=lambda d: d["defaults"].cumsum(),
        cum_n=lambda d: d["n"].cumsum(),
    )
    .reset_index()
)
print("\n=== Risk Decile Table ===")
print(decile_table[["risk_decile","n","defaults","default_rate","avg_prob","cum_defaults"]].to_string(index=False))

# COMMAND ----------
# %md
# ## Cell 8 — Visualisations

fig, axes = plt.subplots(2, 3, figsize=(18, 10))
fig.patch.set_facecolor("#0d1117")
for ax in axes.flat:
    ax.set_facecolor("#161b22")
    ax.tick_params(colors="#8b949e")
    ax.xaxis.label.set_color("#8b949e")
    ax.yaxis.label.set_color("#8b949e")
    ax.title.set_color("#e6edf3")
    for spine in ax.spines.values():
        spine.set_edgecolor("#30363d")

COLOURS = {"GBM":"#3fb950","Random Forest":"#58a6ff","Logistic Regression":"#d29922"}

# ── ROC curves ──────────────────────────────────────────────────────────────
from sklearn.metrics import roc_curve
ax = axes[0, 0]
for name, res in results.items():
    fpr, tpr, _ = roc_curve(y_test, res["y_prob"])
    ax.plot(fpr, tpr, label=f"{name} (AUC={res['auc']:.3f})", color=COLOURS[name], lw=2)
ax.plot([0,1],[0,1],"--",color="#30363d",lw=1)
ax.set_title("ROC Curves — Credit Risk", fontweight="bold")
ax.set_xlabel("False Positive Rate"); ax.set_ylabel("True Positive Rate")
ax.legend(fontsize=9, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")

# ── Precision-Recall ────────────────────────────────────────────────────────
from sklearn.metrics import precision_recall_curve
ax = axes[0, 1]
for name, res in results.items():
    prec, rec, _ = precision_recall_curve(y_test, res["y_prob"])
    ax.plot(rec, prec, label=f"{name} (AP={res['ap']:.3f})", color=COLOURS[name], lw=2)
baseline = y_test.mean()
ax.axhline(baseline, ls="--", color="#30363d", lw=1, label=f"Baseline ({baseline:.2f})")
ax.set_title("Precision-Recall Curves", fontweight="bold")
ax.set_xlabel("Recall"); ax.set_ylabel("Precision")
ax.legend(fontsize=9, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")

# ── Feature Importance ──────────────────────────────────────────────────────
ax = axes[0, 2]
if hasattr(best["pipeline"].named_steps["clf"], "feature_importances_"):
    imp = best["pipeline"].named_steps["clf"].feature_importances_
    feat_df = pd.DataFrame({"feature": FEATURES, "imp": imp}).sort_values("imp")
    colours_fi = ["#f85149" if i > len(feat_df)-4 else "#58a6ff" for i in range(len(feat_df))]
    ax.barh(feat_df["feature"], feat_df["imp"], color=colours_fi)
    ax.set_title(f"Feature Importance ({best_name})", fontweight="bold")

# ── Calibration ─────────────────────────────────────────────────────────────
ax = axes[1, 0]
for name, res in results.items():
    frac_pos, mean_pred = calibration_curve(y_test, res["y_prob"], n_bins=8)
    ax.plot(mean_pred, frac_pos, "s-", label=name, color=COLOURS[name], lw=2, ms=5)
ax.plot([0,1],[0,1],"--",color="#30363d",lw=1,label="Perfect calibration")
ax.set_title("Calibration Curves", fontweight="bold")
ax.set_xlabel("Mean Predicted Probability"); ax.set_ylabel("Fraction of Positives")
ax.legend(fontsize=9, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")

# ── Risk Decile Default Rate ─────────────────────────────────────────────────
ax = axes[1, 1]
bar_colours = ["#3fb950" if r < 0.15 else "#d29922" if r < 0.30 else "#f85149"
               for r in decile_table["default_rate"]]
ax.bar(decile_table["risk_decile"], decile_table["default_rate"] * 100,
       color=bar_colours, edgecolor="#30363d")
ax.set_title("Default Rate by Risk Decile", fontweight="bold")
ax.set_xlabel("Risk Decile (D1=lowest risk)"); ax.set_ylabel("Default Rate %")
ax.axhline(y_test.mean() * 100, ls="--", color="#8b949e", lw=1, label=f"Overall avg {y_test.mean():.1%}")
ax.legend(fontsize=9, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")

# ── Score Distribution ───────────────────────────────────────────────────────
ax = axes[1, 2]
for label, col in [(0,"#3fb950"),(1,"#f85149")]:
    mask = (y_test == label)
    ax.hist(best["y_prob"][mask], bins=30, alpha=0.55,
            label=("Performing" if label==0 else "Default"), color=col, density=True)
ax.axvline(0.30, ls="--", color="#d29922", lw=1.5, label="Cutoff 0.30")
ax.set_title("Score Distribution by Class", fontweight="bold")
ax.set_xlabel("Default Probability"); ax.set_ylabel("Density")
ax.legend(fontsize=9, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")

plt.suptitle("FASTA Credit Risk Model — Fabric ML Simulation", fontsize=14,
             fontweight="bold", color="#e6edf3", y=1.01)
plt.tight_layout()
fig.savefig(os.path.join(ARTIFACTS, "credit_risk_full_report.png"), dpi=150,
            bbox_inches="tight", facecolor="#0d1117")
plt.close()
print("Saved: credit_risk_full_report.png")

# COMMAND ----------
# %md
# ## Cell 9 — Save Metrics + Decile Table

metrics = {
    "notebook": "nb_fabric_ml_01_credit_risk",
    "run_date": datetime.now().isoformat(),
    "mlflow_run_id": run_id,
    "best_model": best_name,
    "features_used": FEATURES,
    "train_size": int(len(X_train)),
    "test_size": int(len(X_test)),
    "default_rate_overall": round(float(y.mean()), 4),
    "models": {
        name: {
            "auc":          round(float(res["auc"]), 4),
            "avg_precision":round(float(res["ap"]),  4),
            "brier_score":  round(float(res["brier"]),4),
            "cv_auc_mean":  round(float(res["cv_auc_mean"]),4),
            "cv_auc_std":   round(float(res["cv_auc_std"]), 4),
        }
        for name, res in results.items()
    },
    "risk_decile_table": decile_table[
        ["risk_decile","n","defaults","default_rate","avg_prob"]
    ].to_dict(orient="records"),
    "fabric_deployment": {
        "model_registry":  "fasta.models.credit_risk",
        "serving_endpoint":"https://adb-xxx.azuredatabricks.net/serving-endpoints/credit-risk/invocations",
        "fabric_notebook": "/Workspaces/FASTA_DW/Notebooks/nb_fabric_ml_01_credit_risk",
        "schedule":        "Saturday 02:00 SAST (weekly retrain)",
        "real_time_scoring":"Via Fabric Real-Time Intelligence event stream on new application events"
    }
}

path = os.path.join(ARTIFACTS, "credit_risk_metrics.json")
with open(path, "w") as f:
    json.dump(metrics, f, indent=2, default=str)
print(f"Saved: credit_risk_metrics.json\n")
print(f"Best model AUC:          {best['auc']:.4f}")
print(f"Best model CV-AUC:       {best['cv_auc_mean']:.4f} +/- {best['cv_auc_std']:.4f}")
print(f"Best model Avg Precision:{best['ap']:.4f}")
print(f"Best model Brier Score:  {best['brier']:.4f}")
