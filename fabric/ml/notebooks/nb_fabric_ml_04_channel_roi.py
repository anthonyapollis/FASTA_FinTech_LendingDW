# Fabric ML Notebook 4 — Channel ROI + Marketing Mix Model
import os, json, warnings
from datetime import datetime
import numpy as np, pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from sklearn.ensemble import GradientBoostingClassifier, GradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, r2_score, mean_absolute_error
warnings.filterwarnings("ignore")

BASE      = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
BRONZE    = os.path.join(BASE, "data", "bronze")
ARTIFACTS = os.path.join(BASE, "fabric", "ml", "artifacts")
os.makedirs(ARTIFACTS, exist_ok=True)

# ── Load ──────────────────────────────────────────────────────────────────────
apps      = pd.read_csv(os.path.join(BRONZE,"loan_applications.csv")).drop_duplicates("application_id")
contracts = pd.read_csv(os.path.join(BRONZE,"loan_contracts.csv")).drop_duplicates("application_id")
checks    = pd.read_csv(os.path.join(BRONZE,"credit_checks.csv")).drop_duplicates("application_id")
afford    = pd.read_csv(os.path.join(BRONZE,"affordability_assessments.csv")).drop_duplicates("application_id")
sessions  = pd.read_csv(os.path.join(BRONZE,"web_sessions.csv")).drop_duplicates("session_id")
payments  = pd.read_csv(os.path.join(BRONZE,"payments.csv")).drop_duplicates("payment_id")
sched     = pd.read_csv(os.path.join(BRONZE,"repayment_schedule.csv"))
campaigns = pd.read_csv(os.path.join(BRONZE,"campaigns.csv")).drop_duplicates("campaign_id")

def norm(s): return str(s).strip().upper() if pd.notna(s) else "UNKNOWN"

# ── Payment collection stats per loan ────────────────────────────────────────
pay_stats = (
    payments.groupby("loan_id")
    .agg(
        total_paid   = ("payment_amount_zar", lambda x: x.clip(lower=0).sum()),
        failed_count = ("payment_status",     lambda x: (x.str.upper().str.strip()=="FAILED").sum()),
        success_rate = ("payment_status",     lambda x: (x.str.upper().str.strip()=="SUCCESS").mean()),
    ).reset_index()
)

sched_stats = (
    sched.groupby("loan_id")
    .agg(
        scheduled_total = ("instalment_amount_zar", lambda x: x.clip(lower=0).sum()),
        overdue_count   = ("schedule_status",        lambda x: x.str.upper().str.strip().isin(["OVERDUE"]).sum()),
    ).reset_index()
)

# ── Build channel-level feature table ────────────────────────────────────────
checks["risk_enc"]  = checks["risk_band"].apply(norm).map({"LOW":1,"MEDIUM":2,"HIGH":3,"VERY_HIGH":4}).fillna(0)
checks["cc_pass"]   = (checks["enquiry_result"].apply(norm)=="PASS").astype(int)
checks["score_cln"] = checks["credit_score"].clip(300,850)
afford["afford_pass"]= (afford["assessment_result"].apply(norm)=="PASS").astype(int)
afford["income_cln"] = afford["net_income_zar"].clip(lower=0)
contracts["status_std"] = contracts["loan_status"].apply(norm)
contracts["default_lbl"]= contracts["status_std"].isin(["ARREARS","WRITTEN_OFF"]).astype(int)

full = (
    apps[["application_id","customer_id","channel_name"]]
    .merge(checks[["application_id","risk_enc","cc_pass","score_cln","adverse_listings_count"]], on="application_id", how="left")
    .merge(afford[["application_id","afford_pass","income_cln","dsr_ratio"]], on="application_id", how="left")
    .merge(contracts[["application_id","loan_id","principal_amount_zar","default_lbl"]], on="application_id", how="left")
    .merge(pay_stats,   on="loan_id", how="left")
    .merge(sched_stats, on="loan_id", how="left")
)
full["channel_std"] = full["channel_name"].apply(norm)
full["collection_rate"] = (full["total_paid"] / full["scheduled_total"].clip(lower=1)).clip(0,1)

# ── Channel quality scorecard ─────────────────────────────────────────────────
channel_agg = (
    full[full["loan_id"].notna()]
    .groupby("channel_std")
    .agg(
        n_loans           = ("loan_id","count"),
        default_rate      = ("default_lbl","mean"),
        avg_credit_score  = ("score_cln","mean"),
        pct_low_risk      = ("risk_enc", lambda x: (x<=2).mean()),
        avg_income        = ("income_cln","mean"),
        avg_dsr           = ("dsr_ratio","mean"),
        avg_collection    = ("collection_rate","mean"),
        avg_principal     = ("principal_amount_zar","mean"),
        pct_afford_pass   = ("afford_pass","mean"),
    )
    .reset_index()
)

# Composite quality score
channel_agg["quality_score"] = (
    (1 - channel_agg["default_rate"])       * 0.35 +
    channel_agg["pct_low_risk"]             * 0.25 +
    (channel_agg["avg_collection"].fillna(0))* 0.20 +
    (channel_agg["avg_credit_score"].fillna(580)/850) * 0.20
).round(4)

channel_agg = channel_agg.sort_values("quality_score", ascending=False).reset_index(drop=True)
channel_agg["rank"] = range(1, len(channel_agg)+1)

print("=== Channel Quality Rankings ===")
print(channel_agg[["rank","channel_std","n_loans","default_rate","quality_score","avg_collection"]].to_string(index=False))

# ── Budget Optimiser ──────────────────────────────────────────────────────────
# If we have R500,000 budget, allocate proportional to quality_score × volume weight
TOTAL_BUDGET = 500_000
channel_agg["vol_weight"]     = channel_agg["n_loans"] / channel_agg["n_loans"].sum()
channel_agg["combined_weight"]= (channel_agg["quality_score"]*0.7 + channel_agg["vol_weight"]*0.3)
channel_agg["combined_weight"]/= channel_agg["combined_weight"].sum()
channel_agg["recommended_budget_zar"] = (channel_agg["combined_weight"] * TOTAL_BUDGET).round(-2)

print(f"\n=== Budget Optimisation (Total R{TOTAL_BUDGET:,.0f}) ===")
print(channel_agg[["channel_std","quality_score","recommended_budget_zar"]].to_string(index=False))

# ── Campaign ROI Model (predict disbursal probability from campaign features) ─
camps_m = campaigns.copy()
camps_m["spend_zar"]   = camps_m["spend_zar"].clip(lower=0)
camps_m["budget_zar"]  = camps_m["budget_zar"].clip(lower=0)
camps_m["clicks"]      = camps_m["clicks"].fillna(0)
camps_m["impressions"] = camps_m["impressions"].fillna(1)
camps_m["ctr"]         = (camps_m["clicks"] / camps_m["impressions"]).clip(0,0.5)
camps_m["spend_ratio"] = (camps_m["spend_zar"] / camps_m["budget_zar"].clip(lower=1)).clip(0,2)
camps_m["is_paid"]     = camps_m["channel_name"].apply(
    lambda x: 1 if str(x).upper() in ("GOOGLE SPONSORED SEARCH","FACEBOOK","EMAIL") else 0
)

# ── Visualisations ────────────────────────────────────────────────────────────
PALETTE = ["#3fb950","#58a6ff","#d29922","#f85149","#bc8cff",
           "#fb8f44","#39d353","#ff7b72","#a5d6ff","#ffa657"]

fig, axes = plt.subplots(2, 3, figsize=(18, 10))
fig.patch.set_facecolor("#0d1117")
for ax in axes.flat:
    ax.set_facecolor("#161b22"); ax.tick_params(colors="#8b949e")
    ax.xaxis.label.set_color("#8b949e"); ax.yaxis.label.set_color("#8b949e")
    ax.title.set_color("#e6edf3")
    for sp in ax.spines.values(): sp.set_edgecolor("#30363d")

# 1 Quality score horizontal bar
ax = axes[0,0]
chs = channel_agg["channel_std"].str.replace(" SPONSORED","\nSPONS.").str.replace("ORGANIC ","ORG\n")
colours = [PALETTE[i % len(PALETTE)] for i in range(len(channel_agg))]
ax.barh(chs, channel_agg["quality_score"]*100, color=colours, edgecolor="#30363d", height=0.6)
ax.set_title("Channel Quality Score", fontweight="bold", color="#e6edf3")
ax.set_xlabel("Score (higher = better loan quality)")
for i, v in enumerate(channel_agg["quality_score"]):
    ax.text(v*100+0.3, i, f"{v:.3f}", va="center", color="#e6edf3", fontsize=9)

# 2 Default rate vs collection rate bubble
ax = axes[0,1]
sizes = (channel_agg["n_loans"] / channel_agg["n_loans"].max() * 800).clip(lower=80)
sc = ax.scatter(channel_agg["default_rate"]*100,
                channel_agg["avg_collection"].fillna(0)*100,
                s=sizes, c=channel_agg["quality_score"],
                cmap="RdYlGn", vmin=0.4, vmax=0.8,
                edgecolors="#30363d", linewidths=0.8, alpha=0.85, zorder=3)
for _, row in channel_agg.iterrows():
    ax.annotate(row["channel_std"][:10],
                (row["default_rate"]*100, (row["avg_collection"] or 0)*100),
                fontsize=7, color="#e6edf3", ha="center", va="bottom")
ax.set_xlabel("Default Rate %"); ax.set_ylabel("Collection Rate %")
ax.set_title("Default vs Collection Rate\n(bubble=volume, colour=quality)", fontweight="bold")
plt.colorbar(sc, ax=ax, label="Quality Score")

# 3 Recommended budget allocation
ax = axes[0,2]
wedge_cols = PALETTE[:len(channel_agg)]
wedges, texts, autotexts = ax.pie(
    channel_agg["recommended_budget_zar"],
    labels=channel_agg["channel_std"].str[:12],
    colors=wedge_cols, autopct="%1.1f%%", startangle=140,
    pctdistance=0.75, textprops={"color":"#e6edf3","fontsize":8}
)
for wt in autotexts: wt.set_fontsize(8); wt.set_color("#0d1117")
ax.set_title(f"Recommended Budget Split\n(Total R{TOTAL_BUDGET:,.0f})", fontweight="bold")

# 4 Credit score by channel
ax = axes[1,0]
score_by_ch = full.groupby("channel_std")["score_cln"].mean().sort_values(ascending=True)
bar_c = ["#3fb950" if v>=640 else "#d29922" if v>=580 else "#f85149" for v in score_by_ch]
ax.barh(score_by_ch.index.str[:15], score_by_ch.values, color=bar_c, edgecolor="#30363d")
ax.axvline(580, ls="--", color="#d29922", lw=1, label="Medium threshold")
ax.axvline(650, ls="--", color="#3fb950", lw=1, label="Low-risk threshold")
ax.set_title("Avg Credit Score by Channel", fontweight="bold")
ax.set_xlabel("Average Credit Score")
ax.legend(fontsize=8, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")

# 5 Volume + quality stacked comparison
ax = axes[1,1]
x_pos = np.arange(len(channel_agg))
w = 0.4
bars1 = ax.bar(x_pos - w/2, channel_agg["n_loans"],      w, color="#58a6ff", alpha=0.85, label="Loan Volume", edgecolor="#30363d")
ax2b  = ax.twinx()
ax2b.set_facecolor("#161b22")
bars2 = ax2b.bar(x_pos + w/2, channel_agg["quality_score"]*100, w, color="#3fb950", alpha=0.85, label="Quality Score×100", edgecolor="#30363d")
ax.set_xticks(x_pos); ax.set_xticklabels(channel_agg["channel_std"].str[:10], rotation=30, color="#8b949e", fontsize=8)
ax.set_ylabel("Loan Volume", color="#58a6ff"); ax2b.set_ylabel("Quality Score×100", color="#3fb950")
ax.set_title("Volume vs Quality by Channel", fontweight="bold")
lines  = [mpatches.Patch(color="#58a6ff",label="Volume"), mpatches.Patch(color="#3fb950",label="Quality×100")]
ax.legend(handles=lines, fontsize=8, facecolor="#21262d", edgecolor="#30363d", labelcolor="#e6edf3")
ax2b.tick_params(colors="#3fb950"); ax2b.spines["right"].set_edgecolor("#30363d")

# 6 Income by channel
ax = axes[1,2]
inc_by_ch = full.groupby("channel_std")["income_cln"].mean().sort_values(ascending=False)
bar_ic = [PALETTE[i % len(PALETTE)] for i in range(len(inc_by_ch))]
ax.bar(inc_by_ch.index.str[:12], inc_by_ch.values/1000, color=bar_ic, edgecolor="#30363d")
ax.set_title("Avg Customer Income by Channel", fontweight="bold")
ax.set_ylabel("Net Income (R000s)")
ax.tick_params(axis="x", rotation=30, labelsize=8, colors="#8b949e")

plt.suptitle("FASTA — Channel ROI & Marketing Mix Model (Fabric ML)",
             fontsize=14, fontweight="bold", color="#e6edf3", y=1.01)
plt.tight_layout()
fig.savefig(os.path.join(ARTIFACTS,"channel_roi_report.png"), dpi=150, bbox_inches="tight", facecolor="#0d1117")
plt.close()
print("Saved: channel_roi_report.png")

# ── Save JSON ─────────────────────────────────────────────────────────────────
out = {
    "notebook": "nb_fabric_ml_04_channel_roi",
    "run_date": datetime.now().isoformat(),
    "total_budget_optimised": TOTAL_BUDGET,
    "channel_quality_table": channel_agg[
        ["rank","channel_std","n_loans","default_rate","quality_score",
         "avg_collection","avg_credit_score","recommended_budget_zar"]
    ].fillna(0).round(4).to_dict(orient="records"),
}
with open(os.path.join(ARTIFACTS,"channel_roi_metrics.json"),"w") as f:
    json.dump(out, f, indent=2, default=str)
print("Saved: channel_roi_metrics.json\nChannel ROI notebook complete.")
