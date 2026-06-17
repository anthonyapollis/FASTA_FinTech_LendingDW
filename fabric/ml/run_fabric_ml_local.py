"""
Local Fabric ML Pipeline Simulator
===================================
Mimics the Microsoft Fabric pipeline execution order:
  pl_01_bronze_ingest   -> already done (generate_bronze_data.py)
  pl_02_silver_cleanse  -> local pandas simulation
  pl_03_gold_marts      -> local aggregation
  pl_04_ml_scoring      -> 4 ML notebooks in dependency order

Run: python run_fabric_ml_local.py
"""
import os, sys, time, json, traceback
from datetime import datetime

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
NB   = os.path.join(os.path.dirname(__file__), "notebooks")
ART  = os.path.join(os.path.dirname(__file__), "artifacts")
os.makedirs(ART, exist_ok=True)

RUN_DATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
PIPELINE_LOG = []

PALETTE = {
    "header":  "\033[1;96m",   # bold cyan
    "ok":      "\033[1;92m",   # bold green
    "fail":    "\033[1;91m",   # bold red
    "warn":    "\033[1;93m",   # bold yellow
    "step":    "\033[1;95m",   # bold magenta
    "info":    "\033[0;94m",   # blue
    "dim":     "\033[2;37m",   # dim grey
    "reset":   "\033[0m",
}
P = PALETTE

def banner(txt, colour="header"):
    w = 72
    print(f"\n{P[colour]}{'='*w}")
    print(f"  {txt}")
    print(f"{'='*w}{P['reset']}")

def step(name, colour="step"):
    print(f"\n{P[colour]}>> {name}{P['reset']}")

def ok(msg):   print(f"  {P['ok']}[OK]   {msg}{P['reset']}")
def fail(msg): print(f"  {P['fail']}[FAIL] {msg}{P['reset']}")
def info(msg): print(f"  {P['info']}[INFO] {msg}{P['reset']}")
def warn(msg): print(f"  {P['warn']}[WARN] {msg}{P['reset']}")

def run_notebook(nb_file, label):
    nb_path = os.path.join(NB, nb_file)
    step(f"Notebook: {label}")
    start = time.time()
    status = "SUCCESS"
    try:
        with open(nb_path, "r", encoding="utf-8") as f:
            code = f.read()
        exec(compile(code, nb_path, "exec"), {"__file__": nb_path, "__name__": "__main__"})
        elapsed = time.time() - start
        ok(f"Completed in {elapsed:.1f}s")
    except Exception as e:
        elapsed = time.time() - start
        status = "FAILED"
        fail(f"Error after {elapsed:.1f}s — {type(e).__name__}: {e}")
        traceback.print_exc()
    PIPELINE_LOG.append({
        "notebook": nb_file, "label": label,
        "status": status, "elapsed_sec": round(elapsed, 2),
        "run_at": datetime.now().isoformat()
    })
    return status == "SUCCESS"

def check_bronze():
    step("Validate Bronze data exists")
    bronze = os.path.join(BASE, "data", "bronze")
    required = ["customers.csv","loan_applications.csv","credit_checks.csv",
                "affordability_assessments.csv","loan_contracts.csv","payments.csv",
                "repayment_schedule.csv","web_sessions.csv","web_events.csv","campaigns.csv"]
    missing = [f for f in required if not os.path.exists(os.path.join(bronze, f))]
    if missing:
        fail(f"Missing Bronze files: {missing}")
        info("Run: python data/bronze/generate_bronze_data.py first")
        return False
    import csv
    total = 0
    for fn in required:
        with open(os.path.join(bronze, fn), encoding="utf-8") as f:
            rows = sum(1 for _ in csv.reader(f)) - 1
        total += rows
        ok(f"{fn:<40} {rows:>7,} rows")
    info(f"Total Bronze rows: {total:,}")
    return True

def silver_simulate():
    """Quick pandas-based Silver simulation (validates DQ rules without full Spark)."""
    step("Silver Layer — DQ Simulation (local pandas)")
    import pandas as pd
    bronze = os.path.join(BASE, "data", "bronze")

    dq_report = {}
    tables = {
        "customers":              ("customer_id", ["net_income_zar"]),
        "loan_applications":      ("application_id", ["application_status"]),
        "credit_checks":          ("credit_check_id", ["credit_score"]),
        "affordability_assessments": ("assessment_id", ["net_income_zar"]),
        "loan_contracts":         ("loan_id", ["principal_amount_zar"]),
        "payments":               ("payment_id", ["payment_amount_zar"]),
        "repayment_schedule":     ("schedule_id", ["instalment_amount_zar"]),
        "web_sessions":           ("session_id", []),
        "web_events":             ("event_id", ["event_value_zar"]),
    }

    for table, (pk, amount_cols) in tables.items():
        path = os.path.join(bronze, f"{table}.csv")
        if not os.path.exists(path): continue
        df = pd.read_csv(path)
        raw_count   = len(df)
        dedup_count = df.drop_duplicates(pk).shape[0]
        dupes       = raw_count - dedup_count
        nulls       = {c: int(df[c].isna().sum()) for c in amount_cols if c in df.columns}
        negatives   = {c: int((pd.to_numeric(df[c], errors="coerce").dropna() < 0).sum()) for c in amount_cols if c in df.columns}

        # Bot sessions
        bots = 0
        if "is_bot" in df.columns:
            bots = int((df["is_bot"] == 1).sum())

        dq_report[table] = {
            "raw_rows": raw_count, "after_dedup": dedup_count,
            "duplicates_removed": dupes, "null_amounts": nulls,
            "negative_amounts": negatives, "bot_rows_removed": bots
        }
        ok(f"{table:<40} raw={raw_count:>6,}  dedup={dedup_count:>6,}  "
           f"dupes={dupes:>4}  bots={bots:>3}")

    # Future payment dates
    pay = pd.read_csv(os.path.join(bronze,"payments.csv"))
    for fmt in ["%Y-%m-%d","%d/%m/%Y","%m-%d-%Y","%Y%m%d"]:
        pay["dt"] = pd.to_datetime(pay["payment_date"], format=fmt, errors="coerce")
        if pay["dt"].notna().sum() > len(pay)*0.5: break
    future = int((pay["dt"] > pd.Timestamp("2026-06-16")).sum())
    warn(f"Future payment dates found: {future} (will be nulled in Silver)")
    dq_report["payments"]["future_dates_nulled"] = future

    with open(os.path.join(ART,"silver_dq_report.json"),"w") as f:
        json.dump(dq_report, f, indent=2, default=str)
    ok("DQ report saved: silver_dq_report.json")
    return True

# ── Main pipeline ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    banner(f"FASTA FinTech — Fabric ML Local Pipeline Runner")
    banner(f"Run date: {RUN_DATE} | Base: {BASE}", colour="info")

    overall_start = time.time()
    steps_ok = 0
    steps_fail = 0

    # Stage 1 — Bronze validation
    banner("STAGE 1 — Bronze Validation", "info")
    if not check_bronze():
        sys.exit(1)
    steps_ok += 1

    # Stage 2 — Silver DQ simulation
    banner("STAGE 2 — Silver Cleansing Simulation", "info")
    if silver_simulate():
        steps_ok += 1
    else:
        steps_fail += 1
        warn("Silver simulation failed — continuing to ML stage")

    # Stage 3 — Gold + ML notebooks
    banner("STAGE 3 — ML Notebooks (Fabric Simulation)", "info")
    info("Executing notebooks in dependency order...")
    info("Notebook 1 of 4 — Credit Risk Classifier")
    nb1 = run_notebook("nb_fabric_ml_01_credit_risk.py",    "Credit Risk Classifier")
    steps_ok += nb1; steps_fail += (not nb1)

    info("Notebook 2 of 4 — Affordability Regression")
    nb2 = run_notebook("nb_fabric_ml_02_affordability.py",  "Affordability Regression + Stress Test")
    steps_ok += nb2; steps_fail += (not nb2)

    info("Notebook 3 of 4 — Churn & Collections")
    nb3 = run_notebook("nb_fabric_ml_03_churn_collections.py","Churn + Promise-to-Pay")
    steps_ok += nb3; steps_fail += (not nb3)

    info("Notebook 4 of 4 — Channel ROI")
    nb4 = run_notebook("nb_fabric_ml_04_channel_roi.py",    "Channel ROI + Budget Optimiser")
    steps_ok += nb4; steps_fail += (not nb4)

    # ── Pipeline summary ──────────────────────────────────────────────────────
    elapsed_total = time.time() - overall_start
    banner("PIPELINE SUMMARY", "header")
    print(f"  {P['ok']}Steps passed : {steps_ok}{P['reset']}")
    print(f"  {P['fail']}Steps failed: {steps_fail}{P['reset']}")
    print(f"  {P['info']}Total time  : {elapsed_total:.1f}s{P['reset']}")

    print(f"\n  {P['dim']}Artifacts written:{P['reset']}")
    for fn in sorted(os.listdir(ART)):
        size = os.path.getsize(os.path.join(ART,fn))
        print(f"    {P['dim']}{fn:<45} {size/1024:>6.1f} KB{P['reset']}")

    # Save pipeline run log
    log = {
        "pipeline": "FASTA_FinTech_FabricML_Local",
        "run_date": RUN_DATE, "total_seconds": round(elapsed_total,2),
        "steps_ok": steps_ok, "steps_fail": steps_fail,
        "notebook_runs": PIPELINE_LOG,
        "fabric_equivalent": {
            "workspace":  "FASTA_DW",
            "pipelines":  ["pl_01_fasta_bronze_ingest","pl_02_fasta_silver_cleanse",
                           "pl_03_fasta_gold_marts","pl_04_fasta_ml_scoring"],
            "lakehouse":  "FastaLakehouse",
            "ml_registry":"fasta.models.*",
        }
    }
    with open(os.path.join(ART,"pipeline_run_log.json"),"w") as f:
        json.dump(log, f, indent=2, default=str)

    final = "SUCCESS" if steps_fail == 0 else "PARTIAL"
    col   = "ok" if steps_fail == 0 else "warn"
    banner(f"Pipeline {final} — {steps_ok} steps passed, {steps_fail} failed", col)
