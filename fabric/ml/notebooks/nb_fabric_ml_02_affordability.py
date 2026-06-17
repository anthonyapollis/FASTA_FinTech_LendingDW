# =============================================================================
# Microsoft Fabric ML Notebook — nb_fabric_ml_02_affordability
# FASTA FinTech — Affordability Amount Regression + Instalment Stress Test
# =============================================================================
# Purpose : Predict maximum affordable monthly instalment per customer
#           + stress-test affordability under income shock scenarios
# Output  : Regression metrics JSON, stress scenario table, plots

import os, sys, json, warnings
from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split, cross_val_score, KFold
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import Ridge, ElasticNet
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.inspection import permutation_importance
warnings.filterwarnings("ignore")

try:
    import mlflow, mlflow.sklearn
    mlflow.set_tracking_uri("file:./mlruns")
    mlflow.set_experiment("FASTA_Affordability")
    MLFLOW = True
except ImportError:
    MLFLOW = False

BASE      = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
BRONZE    = os.path.join(BASE, "data", "bronze")
ARTIFACTS = os.path.join(BASE, "fabric", "ml", "artifacts")
os.makedirs(ARTIFACTS, exist_ok=True)

# COMMAND ----------
# %md ## Cell 2 — Load & Feature Engineering

afford    = pd.read_csv(os.path.join(BRONZE, "affordability_assessments.csv"))
customers = pd.read_csv(os.path.join(BRONZE, "customers.csv")).drop_duplicates("customer_id")
contracts = pd.read_csv(os.path.join(BRONZE, "loan_contracts.csv")).drop_duplicates("application_id")

afford = afford.drop_duplicates("assessment_id").dropna(subset=["affordability_amount_zar"])
afford = afford[afford["affordability_amount_zar"] > 0]
afford["net_income_zar"]        = afford["net_income_zar"].clip(lower=0)
afford["monthly_expenses_zar"]  = afford["monthly_expenses_zar"].clip(lower=0)
afford["existing_debt_zar"]     = afford["existing_debt_zar"].clip(lower=0)
afford["affordability_zar"]     = afford["affordability_amount_zar"].clip(lower=0)

# Derived ratios
afford["expense_ratio"]   = (afford["monthly_expenses_zar"] / afford["net_income_zar"]).clip(0, 1.5)
afford["debt_ratio"]      = (afford["existing_debt_zar"]    / afford["net_income_zar"]).clip(0, 1.5)
afford["disposable_zar"]  = (afford["net_income_zar"] - afford["monthly_expenses_zar"]).clip(lower=0)
afford["afford_pct"]      = (afford["affordability_zar"] / afford["net_income_zar"]).clip(0, 1)

# Income bands (for segment analysis)
afford["income_band"] = pd.cut(
    afford["net_income_zar"].fillna(0),
    bins=[0, 8000, 15000, 25000, 40000, 999999],
    labels=["<8K","8K-15K","15K-25K","25K-40K","40K+"]
).astype(str)

FEATURES = [
    "net_income_zar",
    "monthly_expenses_zar",
    "existing_debt_zar",
    "dsr_ratio",
    "expense_ratio",
    "debt_ratio",
    "disposable_zar",
]

TARGET = "affordability_zar"

df_model = afford.dropna(subset=FEATURES + [TARGET]).copy()
X = df_model[FEATURES]
y = df_model[TARGET]
print(f"Regression dataset: {len(X):,} rows  |  Avg affordability: R{y.mean():,.0f}")

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42)

# COMMAND ----------
# %md ## Cell 3 — Train Multiple Regressors

regressors = {
    "GBM Regressor": Pipeline([
        ("imp",   SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
        ("reg",   GradientBoostingRegressor(
            n_estimators=500, max_depth=4, learning_rate=0.03,
            subsample=0.80, min_samples_leaf=10, random_state=42
        ))
    ]),
    "Random Forest": Pipeline([
        ("imp",   SimpleImputer(strategy="median")),
        ("reg",   RandomForestRegressor(
            n_estimators=300, max_depth=8, min_samples_leaf=5,
            n_jobs=-1, random_state=42
        ))
    ]),
    "ElasticNet": Pipeline([
        ("imp",   SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
        ("poly",  PolynomialFeatures(degree=2, include_bias=False)),
        ("reg",   ElasticNet(alpha=0.01, l1_ratio=0.5, max_iter=2000))
    ]),
}

cv_kf  = KFold(n_splits=5, shuffle=True, random_state=42)
reg_results = {}

print("\n=== Regression Results ===")
for name, pipe in regressors.items():
    cv_r2 = cross_val_score(pipe, X_train, y_train, cv=cv_kf, scoring="r2", n_jobs=-1)
    pipe.fit(X_train, y_train)
    y_pred = pipe.predict(X_test)
    mae    = mean_absolute_error(y_test, y_pred)
    rmse   = np.sqrt(mean_squared_error(y_test, y_pred))
    r2     = r2_score(y_test, y_pred)
    mape   = np.mean(np.abs((y_test - y_pred) / y_test.clip(lower=1))) * 100
    reg_results[name] = {
        "pipeline": pipe, "y_pred": y_pred,
        "mae": mae, "rmse": rmse, "r2": r2, "mape": mape,
        "cv_r2_mean": cv_r2.mean(), "cv_r2_std": cv_r2.std()
    }
    print(f"  {name:<20}  R2={r2:.4f}  CV-R2={cv_r2.mean():.4f}±{cv_r2.std():.4f}  "
          f"MAE=R{mae:,.0f}  RMSE=R{rmse:,.0f}  MAPE={mape:.1f}%")

best_reg_name = max(reg_results, key=lambda k: reg_results[k]["r2"])
best_reg      = reg_results[best_reg_name]
print(f"\nBest regressor: {best_reg_name}  (R2={best_reg['r2']:.4f})")

# COMMAND ----------
# %md ## Cell 4 — Affordability Stress Test Scenarios

print("\n=== Affordability Stress Test Scenarios ===")
SCENARIOS = {
    "Base Case":            {"income_shock": 0.00, "expense_shock": 0.00, "debt_shock": 0.00},
    "10% Income Drop":      {"income_shock": 0.10, "expense_shock": 0.00, "debt_shock": 0.00},
    "20% Income Drop":      {"income_shock": 0.20, "expense_shock": 0.00, "debt_shock": 0.00},
    "30% Income Drop":      {"income_shock": 0.30, "expense_shock": 0.00, "debt_shock": 0.00},
    "Expenses +15%":        {"income_shock": 0.00, "expense_shock": 0.15, "debt_shock": 0.00},
    "Expenses +30%":        {"income_shock": 0.00, "expense_shock": 0.30, "debt_shock": 0.00},
    "New Debt +R2000/mo":   {"income_shock": 0.00, "expense_shock": 0.00, "debt_shock": 2000},
    "Severe: -25% + +20%":  {"income_shock": 0.25, "expense_shock": 0.20, "debt_shock": 1000},
}

stress_rows = []
for scenario, shocks in SCENARIOS.items():
    X_stressed = X_test.copy()
    X_stressed["net_income_zar"]       = X_stressed["net_income_zar"] * (1 - shocks["income_shock"])
    X_stressed["monthly_expenses_zar"] = X_stressed["monthly_expenses_zar"] * (1 + shocks["expense_shock"]) + shocks["debt_shock"]
    X_stressed["existing_debt_zar"]    = X_stressed["existing_debt_zar"] + shocks["debt_shock"]
    # Recalculate ratios
    X_stressed["expense_ratio"]  = (X_stressed["monthly_expenses_zar"] / X_stressed["net_income_zar"]).clip(0, 1.5)
    X_stressed["debt_ratio"]     = (X_stressed["existing_debt_zar"]    / X_stressed["net_income_zar"]).clip(0, 1.5)
    X_stressed["dsr_ratio"]      = X_stressed["debt_ratio"]
    X_stressed["disposable_zar"] = (X_stressed["net_income_zar"] - X_stressed["monthly_expenses_zar"]).clip(lower=0)

    pred_stressed = best_reg["pipeline"].predict(X_stressed)
    avg_afford    = pred_stressed.mean()
    pct_below_500 = (pred_stressed < 500).mean() * 100  # unaffordable
    pct_above_2k  = (pred_stressed > 2000).mean() * 100

    stress_rows.append({
        "Scenario":          scenario,
        "Avg Affordability": f"R{avg_afford:,.0f}",
        "% Unaffordable":    f"{pct_below_500:.1f}%",
        "% >R2K available":  f"{pct_above_2k:.1f}%",
        "avg_afford_raw":    avg_afford,
    })
    print(f"  {scenario:<30}  Avg afford=R{avg_afford:,.0f}  "
          f"Unaffordable={pct_below_500:.1f}%")

stress_df = pd.DataFrame(stress_rows)

# COMMAND ----------
# %md ## Cell 5 — Segment Analysis by Income Band

segment_results = (
    df_model.assign(
        predicted=best_reg["pipeline"].predict(X)
    )
    .groupby("income_band")
    .agg(
        n=("affordability_zar","count"),
        avg_actual_afford=("affordability_zar","mean"),
        avg_predicted_afford=("predicted","mean"),
        median_afford=("affordability_zar","median"),
        pct_below_800=("affordability_zar", lambda x: (x < 800).mean() * 100),
    )
    .reset_index()
    .sort_values("income_band")
)
print("\n=== Affordability by Income Band ===")
print(segment_results.to_string(index=False))

# COMMAND ----------
# %md ## Cell 6 — Visualisations

fig, axes = plt.subplots(2, 3, figsize=(18, 10))
fig.patch.set_facecolor("#0d1117")
for ax in axes.flat:
    ax.set_facecolor("#161b22")
    ax.tick_params(colors="#8b949e"); ax.xaxis.label.set_color("#8b949e")
    ax.yaxis.label.set_color("#8b949e"); ax.title.set_color("#e6edf3")
    for sp in ax.spines.values(): sp.set_edgecolor("#30363d")

RCOLS = {"GBM Regressor":"#3fb950","Random Forest":"#58a6ff","ElasticNet":"#d29922"}

# ── Actual vs Predicted ──────────────────────────────────────────────────────
ax = axes[0, 0]
ax.scatter(y_test, best_reg["y_pred"], alpha=0.25, s=8, color="#58a6ff", label="Predicted")
lim = [0, min(max(y_test), 30000)]
ax.plot(lim, lim, "r--", lw=1.5, label="Perfect fit")
ax.set_title(f"Actual vs Predicted ({best_reg_name})", fontweight="bold")
ax.set_xlabel("Actual Affordability ZAR"); ax.set_ylabel("Predicted ZAR")
ax.text(0.05, 0.90, f"R²={best_reg['r2']:.4f}\nMAE=R{best_reg['mae']:,.0f}",
        transform=ax.transAxes, color="#e6edf3", fontsize=9,
        bbox=dict(boxstyle="round", facecolor="#21262d", alpha=0.8))
ax.legend(fontsize=9, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")

# ── Residuals ───────────────────────────────────────────────────────────────
ax = axes[0, 1]
residuals = y_test - best_reg["y_pred"]
ax.hist(residuals, bins=50, color="#58a6ff", alpha=0.7, edgecolor="#30363d")
ax.axvline(0, ls="--", color="#f85149", lw=1.5)
ax.axvline(residuals.mean(), ls="--", color="#d29922", lw=1.5, label=f"Mean={residuals.mean():.0f}")
ax.set_title("Residual Distribution", fontweight="bold")
ax.set_xlabel("Residual (Actual - Predicted)"); ax.set_ylabel("Count")
ax.legend(fontsize=9, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")

# ── Stress Test Results ──────────────────────────────────────────────────────
ax = axes[0, 2]
colours_s = ["#3fb950" if r["avg_afford_raw"] > 2500 else
             "#d29922" if r["avg_afford_raw"] > 1500 else "#f85149"
             for _, r in stress_df.iterrows()]
bars = ax.barh(stress_df["Scenario"], stress_df["avg_afford_raw"], color=colours_s, edgecolor="#30363d")
ax.axvline(1000, ls="--", color="#f85149", lw=1, label="Minimum viable (R1000)")
ax.set_title("Stress Test — Avg Affordability by Scenario", fontweight="bold")
ax.set_xlabel("Avg Affordable Amount (ZAR)")
ax.legend(fontsize=9, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")

# ── Segment: Affordability by Income Band ───────────────────────────────────
ax = axes[1, 0]
bands   = segment_results["income_band"]
x_pos   = np.arange(len(bands))
width   = 0.35
ax.bar(x_pos - width/2, segment_results["avg_actual_afford"],    width, label="Actual",    color="#3fb950", alpha=0.8)
ax.bar(x_pos + width/2, segment_results["avg_predicted_afford"], width, label="Predicted", color="#58a6ff", alpha=0.8)
ax.set_xticks(x_pos); ax.set_xticklabels(bands, rotation=15, color="#8b949e")
ax.set_title("Avg Affordability by Income Band", fontweight="bold")
ax.set_ylabel("ZAR")
ax.legend(fontsize=9, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")

# ── R² Comparison ───────────────────────────────────────────────────────────
ax = axes[1, 1]
names = list(reg_results.keys())
r2s   = [reg_results[n]["r2"] for n in names]
ax.bar(names, r2s, color=[RCOLS[n] for n in names], edgecolor="#30363d")
ax.set_title("R² Score by Model", fontweight="bold")
ax.set_ylabel("R² Score"); ax.set_ylim(0, 1)
ax.tick_params(axis="x", labelsize=9, rotation=10, colors="#8b949e")
for i, v in enumerate(r2s):
    ax.text(i, v + 0.01, f"{v:.4f}", ha="center", color="#e6edf3", fontsize=9)

# ── Income vs Affordability scatter with trend ───────────────────────────────
ax = axes[1, 2]
sample = df_model.sample(min(800, len(df_model)), random_state=42)
scatter = ax.scatter(sample["net_income_zar"], sample["affordability_zar"],
                     c=sample["dsr_ratio"].fillna(0.3), cmap="RdYlGn_r",
                     alpha=0.4, s=10, vmin=0, vmax=0.8)
z = np.polyfit(sample["net_income_zar"].fillna(0), sample["affordability_zar"].fillna(0), 1)
p = np.poly1d(z)
xs = np.linspace(sample["net_income_zar"].min(), sample["net_income_zar"].max(), 100)
ax.plot(xs, p(xs), "#d29922", lw=2, label="Trend")
ax.set_title("Income vs Affordability (colour=DSR)", fontweight="bold")
ax.set_xlabel("Net Income ZAR"); ax.set_ylabel("Affordability ZAR")
plt.colorbar(scatter, ax=ax, label="DSR Ratio")
ax.legend(fontsize=9, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")

plt.suptitle("FASTA Affordability Regression — Fabric ML Simulation",
             fontsize=14, fontweight="bold", color="#e6edf3", y=1.01)
plt.tight_layout()
fig.savefig(os.path.join(ARTIFACTS, "affordability_full_report.png"), dpi=150,
            bbox_inches="tight", facecolor="#0d1117")
plt.close()
print("Saved: affordability_full_report.png")

# COMMAND ----------
# %md ## Cell 7 — Save Metrics

if MLFLOW:
    with mlflow.start_run(run_name=f"affordability_{best_reg_name.replace(' ','_').lower()}"):
        mlflow.log_param("model_type", best_reg_name)
        mlflow.log_metric("r2",   round(best_reg["r2"],   4))
        mlflow.log_metric("mae",  round(best_reg["mae"],  2))
        mlflow.log_metric("rmse", round(best_reg["rmse"], 2))
        mlflow.log_metric("mape", round(best_reg["mape"], 2))
        mlflow.sklearn.log_model(best_reg["pipeline"], "affordability_model",
                                 registered_model_name="fasta.models.affordability")

metrics_out = {
    "notebook": "nb_fabric_ml_02_affordability",
    "run_date": datetime.now().isoformat(),
    "best_model": best_reg_name,
    "models": {
        n: {"r2": round(v["r2"],4), "mae": round(v["mae"],2),
            "rmse": round(v["rmse"],2), "mape": round(v["mape"],2),
            "cv_r2_mean": round(v["cv_r2_mean"],4)}
        for n, v in reg_results.items()
    },
    "stress_scenarios": stress_df.drop("avg_afford_raw", axis=1).to_dict(orient="records"),
    "segment_analysis": segment_results.to_dict(orient="records"),
}
with open(os.path.join(ARTIFACTS, "affordability_metrics.json"), "w") as f:
    json.dump(metrics_out, f, indent=2, default=str)

print(f"\nAffordability model complete.")
print(f"  Best: {best_reg_name}  R2={best_reg['r2']:.4f}  MAE=R{best_reg['mae']:,.0f}")
