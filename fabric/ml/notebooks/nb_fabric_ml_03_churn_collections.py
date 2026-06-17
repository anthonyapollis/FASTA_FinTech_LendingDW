# =============================================================================
# Microsoft Fabric ML Notebook — nb_fabric_ml_03_churn_collections
# FASTA FinTech — Customer Churn + Collections Propensity Models
# =============================================================================
# Model A: Churn Predictor
#   Definition: Active loan customer with no successful payment in 45+ days
#   Action    : Trigger proactive collections outreach before formal arrears
#
# Model B: Promise-to-Pay Predictor
#   Definition: Of customers contacted by collections, will they pay within 7d?
#   Action    : Route high-propensity customers to self-serve; low to agent
#
# Model C: Early Warning Score (EWS)
#   Definition: Score applied at disbursement that predicts 90-day delinquency
#   Action    : Flag for enhanced monitoring from Day 1

import os, sys, json, warnings
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.ensemble import (GradientBoostingClassifier, RandomForestClassifier,
                               ExtraTreesClassifier)
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (roc_auc_score, average_precision_score,
                              classification_report, confusion_matrix,
                              ConfusionMatrixDisplay)
warnings.filterwarnings("ignore")

try:
    import mlflow, mlflow.sklearn
    mlflow.set_tracking_uri("file:./mlruns")
    mlflow.set_experiment("FASTA_Churn_Collections")
    MLFLOW = True
except ImportError:
    MLFLOW = False

BASE      = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
BRONZE    = os.path.join(BASE, "data", "bronze")
ARTIFACTS = os.path.join(BASE, "fabric", "ml", "artifacts")
os.makedirs(ARTIFACTS, exist_ok=True)

SNAPSHOT = pd.Timestamp("2026-06-16")

# COMMAND ----------
# %md ## Cell 2 — Load Data

contracts  = pd.read_csv(os.path.join(BRONZE, "loan_contracts.csv")).drop_duplicates("loan_id")
payments   = pd.read_csv(os.path.join(BRONZE, "payments.csv")).drop_duplicates("payment_id")
sched      = pd.read_csv(os.path.join(BRONZE, "repayment_schedule.csv"))
collections= pd.read_csv(os.path.join(BRONZE, "collection_actions.csv"))
afford     = pd.read_csv(os.path.join(BRONZE, "affordability_assessments.csv")).drop_duplicates("application_id")
checks     = pd.read_csv(os.path.join(BRONZE, "credit_checks.csv")).drop_duplicates("application_id")
tickets    = pd.read_csv(os.path.join(BRONZE, "support_tickets.csv"))

# Parse payment dates
for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%Y%m%d"]:
    try:
        payments["pay_dt"] = pd.to_datetime(payments["payment_date"], format=fmt, errors="coerce")
        if payments["pay_dt"].notna().sum() > len(payments) * 0.6:
            break
    except Exception:
        pass

contracts["loan_status_std"] = contracts["loan_status"].str.upper().str.strip()

# COMMAND ----------
# %md ## Cell 3 — Model A: Churn Feature Engineering

# Last successful payment per loan
last_pay = (
    payments[payments["payment_status"].str.upper().str.strip() == "SUCCESS"]
    .groupby("loan_id")["pay_dt"].max().reset_index()
    .rename(columns={"pay_dt":"last_payment_date"})
)

# Payment behaviour per loan
pay_stats = (
    payments
    .groupby("loan_id")
    .agg(
        total_payments       = ("payment_id","count"),
        successful_payments  = ("payment_status", lambda x: (x.str.upper().str.strip()=="SUCCESS").sum()),
        failed_payments      = ("payment_status", lambda x: (x.str.upper().str.strip()=="FAILED").sum()),
        total_paid_zar       = ("payment_amount_zar", lambda x: x.clip(lower=0).sum()),
    )
    .reset_index()
    .assign(
        payment_success_rate = lambda d: d["successful_payments"] / d["total_payments"].clip(lower=1)
    )
)

# Schedule stats per loan
sched_stats = (
    sched
    .groupby("loan_id")
    .agg(
        total_instalments   = ("schedule_id","count"),
        overdue_instalments = ("schedule_status", lambda x: x.str.upper().str.strip().isin(["OVERDUE"]).sum()),
        total_scheduled_zar = ("instalment_amount_zar", lambda x: x.clip(lower=0).sum()),
    )
    .reset_index()
    .assign(
        overdue_rate = lambda d: d["overdue_instalments"] / d["total_instalments"].clip(lower=1)
    )
)

# Support ticket count per customer
ticket_counts = (
    tickets[tickets["customer_id"].notna()]
    .groupby("customer_id")
    .agg(ticket_count=("ticket_id","count"))
    .reset_index()
)

# Churn target: active loans, no payment in 45+ days
active_loans = contracts[contracts["loan_status_std"] == "ACTIVE"].copy()
active_loans = active_loans.merge(last_pay, on="loan_id", how="left")
active_loans["days_since_pay"] = (SNAPSHOT - active_loans["last_payment_date"]).dt.days
active_loans["days_since_pay"] = active_loans["days_since_pay"].fillna(999)
active_loans["churn_label"]    = (active_loans["days_since_pay"] > 45).astype(int)

# Merge features
churn_df = (
    active_loans
    .merge(pay_stats,   on="loan_id",     how="left")
    .merge(sched_stats, on="loan_id",     how="left")
    .merge(afford[["application_id","net_income_zar","dsr_ratio","assessment_result"]],
           on="application_id", how="left")
    .merge(checks[["application_id","credit_score","risk_band","adverse_listings_count"]],
           on="application_id", how="left")
    .merge(ticket_counts, on="customer_id", how="left")
)

churn_df["net_income_zar"] = churn_df["net_income_zar"].clip(lower=0)
churn_df["afford_pass"]    = (churn_df["assessment_result"].fillna("").str.upper() == "PASS").astype(int)
churn_df["risk_band_enc"]  = churn_df["risk_band"].fillna("").str.upper().map(
    {"LOW":1,"MEDIUM":2,"HIGH":3,"VERY_HIGH":4}).fillna(0).astype(int)
churn_df["ticket_count"]   = churn_df["ticket_count"].fillna(0)
churn_df["credit_score"]   = churn_df["credit_score"].clip(300, 850)

CHURN_FEATURES = [
    "days_since_pay", "payment_success_rate", "failed_payments",
    "overdue_rate", "overdue_instalments", "total_paid_zar",
    "principal_amount_zar", "term_months",
    "net_income_zar", "dsr_ratio", "risk_band_enc",
    "credit_score", "adverse_listings_count", "ticket_count", "afford_pass"
]

churn_clean = churn_df.dropna(subset=["churn_label"]).copy()
Xc = churn_clean[CHURN_FEATURES]
yc = churn_clean["churn_label"].astype(int)
print(f"\nChurn dataset: {len(Xc):,} rows  |  Churn rate: {yc.mean():.1%}")

# COMMAND ----------
# %md ## Cell 4 — Train Churn Models

cv_strat = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

churn_models = {
    "GBM": Pipeline([
        ("imp",   SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
        ("clf",   GradientBoostingClassifier(
            n_estimators=300, max_depth=3, learning_rate=0.05,
            subsample=0.80, random_state=42
        ))
    ]),
    "Extra Trees": Pipeline([
        ("imp",   SimpleImputer(strategy="median")),
        ("clf",   ExtraTreesClassifier(
            n_estimators=300, max_depth=5, class_weight="balanced",
            random_state=42, n_jobs=-1
        ))
    ]),
}

churn_results = {}
if len(Xc) > 100 and yc.nunique() > 1:
    Xc_train, Xc_test, yc_train, yc_test = train_test_split(
        Xc, yc, test_size=0.20, random_state=42, stratify=yc
    )
    print("\n=== Churn Model Results ===")
    for name, pipe in churn_models.items():
        try:
            cv_auc = cross_val_score(pipe, Xc_train, yc_train, cv=cv_strat,
                                      scoring="roc_auc", n_jobs=-1)
            pipe.fit(Xc_train, yc_train)
            yp = pipe.predict_proba(Xc_test)[:,1]
            auc = roc_auc_score(yc_test, yp) if yc_test.nunique()>1 else 0.5
            ap  = average_precision_score(yc_test, yp)
            churn_results[name] = {"pipeline":pipe,"y_prob":yp,"auc":auc,"ap":ap,
                                    "cv_auc":cv_auc.mean()}
            print(f"  {name:<15}  CV-AUC={cv_auc.mean():.4f}  "
                  f"Test-AUC={auc:.4f}  AP={ap:.4f}")
        except Exception as e:
            print(f"  {name}: skipped ({e})")
else:
    print("Churn dataset too small — skipping model training, using heuristic only")
    churn_results = {}

best_churn = max(churn_results, key=lambda k: churn_results[k]["auc"]) if churn_results else None

# COMMAND ----------
# %md ## Cell 5 — Model B: Promise-to-Pay

# Customers who received a collection action — did they pay within 7 days?
coll = collections.copy()
for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y"]:
    try:
        coll["action_dt"] = pd.to_datetime(coll["action_date"], format=fmt, errors="coerce")
        if coll["action_dt"].notna().sum() > len(coll)*0.6: break
    except Exception: pass

coll = coll.merge(pay_stats, on="loan_id", how="left")
coll = coll.merge(sched_stats, on="loan_id", how="left")
coll = coll.merge(contracts[["loan_id","application_id","principal_amount_zar","term_months",
                               "loan_status_std"]], on="loan_id", how="left")
coll = coll.merge(afford[["application_id","net_income_zar","dsr_ratio"]], on="application_id", how="left")
coll = coll.merge(checks[["application_id","credit_score","risk_band_enc" if "risk_band_enc" in checks.columns else "credit_score"]], on="application_id", how="left")

# Label: outcome mentions payment
coll["ptp_label"] = coll["action_outcome"].fillna("").str.upper().str.contains(
    "PAYMENT|PAY|RECEIVED"
).astype(int)

PTP_FEATURES = [
    "payment_success_rate", "failed_payments", "overdue_rate",
    "total_paid_zar", "principal_amount_zar", "term_months",
    "net_income_zar", "dsr_ratio"
]
ptp_clean = coll.dropna(subset=PTP_FEATURES + ["ptp_label"])
Xp = ptp_clean[PTP_FEATURES]
yp_label = ptp_clean["ptp_label"].astype(int)
print(f"\nPTP dataset: {len(Xp):,} rows  |  PTP rate: {yp_label.mean():.1%}")

ptp_results = {}
if len(Xp) > 80 and yp_label.nunique() > 1:
    Xp_tr, Xp_te, yp_tr, yp_te = train_test_split(Xp, yp_label, test_size=0.20,
                                                     random_state=42, stratify=yp_label)
    ptp_pipe = Pipeline([
        ("imp",   SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
        ("clf",   GradientBoostingClassifier(n_estimators=200, max_depth=3, random_state=42))
    ])
    ptp_pipe.fit(Xp_tr, yp_tr)
    yp_prob = ptp_pipe.predict_proba(Xp_te)[:,1]
    ptp_auc = roc_auc_score(yp_te, yp_prob) if yp_te.nunique()>1 else 0.5
    ptp_ap  = average_precision_score(yp_te, yp_prob)
    ptp_results = {"pipeline":ptp_pipe,"y_prob":yp_prob,"auc":ptp_auc,"ap":ptp_ap}
    print(f"  PTP model: AUC={ptp_auc:.4f}  AP={ptp_ap:.4f}")

# COMMAND ----------
# %md ## Cell 6 — Visualisations

fig, axes = plt.subplots(2, 3, figsize=(18, 10))
fig.patch.set_facecolor("#0d1117")
for ax in axes.flat:
    ax.set_facecolor("#161b22"); ax.tick_params(colors="#8b949e")
    ax.xaxis.label.set_color("#8b949e"); ax.yaxis.label.set_color("#8b949e")
    ax.title.set_color("#e6edf3")
    for sp in ax.spines.values(): sp.set_edgecolor("#30363d")

from sklearn.metrics import roc_curve

# ── Churn ROC ───────────────────────────────────────────────────────────────
ax = axes[0, 0]
CCOLS = {"GBM":"#3fb950","Extra Trees":"#58a6ff"}
if churn_results and "Xc_test" in dir():
    for name, res in churn_results.items():
        fpr, tpr, _ = roc_curve(yc_test, res["y_prob"])
        ax.plot(fpr, tpr, label=f"{name} AUC={res['auc']:.3f}", color=CCOLS.get(name,"#bc8cff"), lw=2)
ax.plot([0,1],[0,1],"--",color="#30363d",lw=1)
ax.set_title("Churn Model — ROC Curves", fontweight="bold")
ax.set_xlabel("FPR"); ax.set_ylabel("TPR")
ax.legend(fontsize=9, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")

# ── Churn by Risk Band ───────────────────────────────────────────────────────
ax = axes[0, 1]
churn_by_risk = (
    churn_clean
    .groupby("risk_band_enc")["churn_label"]
    .agg(["mean","count"])
    .reset_index()
)
churn_by_risk["risk_label"] = churn_by_risk["risk_band_enc"].map(
    {0:"Unknown",1:"LOW",2:"MEDIUM",3:"HIGH",4:"VERY_HIGH"}
)
bar_cols = ["#3fb950","#d29922","#fb8f44","#f85149","#8b949e"]
ax.bar(churn_by_risk["risk_label"], churn_by_risk["mean"]*100,
       color=bar_cols[:len(churn_by_risk)], edgecolor="#30363d")
ax.set_title("Churn Rate by Risk Band", fontweight="bold")
ax.set_ylabel("Churn Rate %")
ax.tick_params(axis="x", colors="#8b949e")

# ── Churn score dist ─────────────────────────────────────────────────────────
ax = axes[0, 2]
if churn_results and best_churn and "yc_test" in dir():
    bp = churn_results[best_churn]["y_prob"]
    for lbl, col in [(0,"#3fb950"),(1,"#f85149")]:
        mask = (yc_test == lbl)
        ax.hist(bp[mask], bins=25, alpha=0.55,
                label=("Not Churned" if lbl==0 else "Churned"), color=col, density=True)
    ax.axvline(0.50, ls="--", color="#d29922", lw=1.5, label="Cutoff 0.50")
ax.set_title("Churn Score Distribution", fontweight="bold")
ax.set_xlabel("Churn Probability"); ax.set_ylabel("Density")
ax.legend(fontsize=9, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")

# ── Days since payment histogram ─────────────────────────────────────────────
ax = axes[1, 0]
dsp = churn_clean["days_since_pay"].clip(0, 200)
ax.hist(dsp[yc==0], bins=30, alpha=0.55, color="#3fb950", label="Performing", density=True)
ax.hist(dsp[yc==1], bins=30, alpha=0.55, color="#f85149", label="Churned",    density=True)
ax.axvline(45, ls="--", color="#d29922", lw=1.5, label="45-day threshold")
ax.set_title("Days Since Last Payment by Churn", fontweight="bold")
ax.set_xlabel("Days"); ax.set_ylabel("Density")
ax.legend(fontsize=9, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")

# ── PTP model ───────────────────────────────────────────────────────────────
ax = axes[1, 1]
if ptp_results and "Xp_te" in dir():
    fpr_p, tpr_p, _ = roc_curve(yp_te, ptp_results["y_prob"])
    ax.plot(fpr_p, tpr_p, color="#bc8cff", lw=2,
            label=f"PTP AUC={ptp_results['auc']:.3f}")
    ax.plot([0,1],[0,1],"--",color="#30363d",lw=1)
ax.set_title("Promise-to-Pay Model — ROC", fontweight="bold")
ax.set_xlabel("FPR"); ax.set_ylabel("TPR")
ax.legend(fontsize=9, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")

# ── Collection Action Outcomes ───────────────────────────────────────────────
ax = axes[1, 2]
outcomes = (coll["action_outcome"]
            .fillna("Unknown")
            .str.split(",").str[0]
            .str.strip()
            .value_counts()
            .head(7))
cols_out = ["#3fb950","#58a6ff","#d29922","#fb8f44","#f85149","#bc8cff","#8b949e"]
ax.barh(outcomes.index, outcomes.values,
        color=cols_out[:len(outcomes)], edgecolor="#30363d")
ax.set_title("Collection Action Outcomes", fontweight="bold")
ax.set_xlabel("Count")

plt.suptitle("FASTA Churn & Collections ML — Fabric Simulation",
             fontsize=14, fontweight="bold", color="#e6edf3", y=1.01)
plt.tight_layout()
fig.savefig(os.path.join(ARTIFACTS, "churn_collections_report.png"), dpi=150,
            bbox_inches="tight", facecolor="#0d1117")
plt.close()
print("Saved: churn_collections_report.png")

# COMMAND ----------
# %md ## Cell 7 — Save Metrics

metrics_out = {
    "notebook": "nb_fabric_ml_03_churn_collections",
    "run_date": datetime.now().isoformat(),
    "churn_model": {
        "definition": "Active loan, no successful payment in 45+ days",
        "dataset_size": int(len(Xc)),
        "churn_rate": round(float(yc.mean()), 4),
        "models": {n: {"auc":round(v["auc"],4),"ap":round(v["ap"],4)}
                   for n,v in churn_results.items()} if churn_results else {},
        "best_model": best_churn,
    },
    "ptp_model": {
        "definition": "Collections contacted customer — will they pay within 7 days?",
        "dataset_size": int(len(Xp)),
        "ptp_rate": round(float(yp_label.mean()), 4),
        "auc": round(float(ptp_results.get("auc", 0.5)), 4) if ptp_results else None,
    },
    "fabric_deployment": {
        "churn_trigger":  "Daily batch on silver.payments — flag loans with days_since_pay > 30",
        "ptp_trigger":    "At each collection action creation — score and route to agent/self-serve",
        "ews_trigger":    "At disbursement — score and write to gold.fact_disbursement_scored",
    }
}
with open(os.path.join(ARTIFACTS, "churn_collections_metrics.json"), "w") as f:
    json.dump(metrics_out, f, indent=2, default=str)
print("Saved: churn_collections_metrics.json")
print("Churn/Collections notebook complete.")
