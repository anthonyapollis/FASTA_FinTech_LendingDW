# Databricks notebook — Silver Layer: Cleanse & Transform
# FASTA / Confluent FinTech Lending DW
# Medallion Architecture: Bronze (raw CSV) -> Silver (clean Delta)
#
# Run in Databricks with:
#   BRONZE_PATH  = /mnt/landing/bronze/fasta/
#   SILVER_PATH  = /mnt/datalake/silver/fasta/
#
# Designed for:
#   - Databricks Runtime 13+ (Delta Lake)
#   - Python / PySpark
#   - Works locally against CSV files for dev/testing

# COMMAND ----------
# %md
# ## 1. Setup & Config

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
import re

# Local dev fallback — replace with dbutils.widgets or Databricks secrets in prod
try:
    dbutils  # noqa: F821
    BRONZE_PATH = dbutils.widgets.get("bronze_path")   # noqa: F821
    SILVER_PATH = dbutils.widgets.get("silver_path")   # noqa: F821
    MODE = "databricks"
except NameError:
    import os
    BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data"))
    BRONZE_PATH = os.path.join(BASE, "bronze")
    SILVER_PATH = os.path.join(BASE, "silver")
    MODE = "local"
    spark = SparkSession.builder.appName("fasta_silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

print(f"Mode: {MODE}")
print(f"Bronze: {BRONZE_PATH}")
print(f"Silver: {SILVER_PATH}")

# COMMAND ----------
# %md
# ## 2. Shared UDFs — Date normalisation & Amount cleaning

DATE_FMTS = [
    "yyyy-MM-dd",
    "yyyy-MM-dd HH:mm:ss",
    "dd/MM/yyyy",
    "MM-dd-yyyy",
    "dd MMM yyyy",
    "yyyyMMdd",
]


def coerce_date(col_name):
    """Try multiple date formats and return a normalised date column."""
    result = F.lit(None).cast(T.DateType())
    for fmt in DATE_FMTS:
        result = F.when(
            result.isNull() & F.to_date(F.col(col_name), fmt).isNotNull(),
            F.to_date(F.col(col_name), fmt)
        ).otherwise(result)
    return result


def coerce_timestamp(col_name):
    ts_fmts = ["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", "dd/MM/yyyy HH:mm:ss"]
    result = F.lit(None).cast(T.TimestampType())
    for fmt in ts_fmts:
        result = F.when(
            result.isNull() & F.to_timestamp(F.col(col_name), fmt).isNotNull(),
            F.to_timestamp(F.col(col_name), fmt)
        ).otherwise(result)
    return result


def clean_amount(col_name):
    """Clip negative amounts to NULL (they are data entry errors)."""
    return F.when(F.col(col_name) < 0, F.lit(None).cast(T.DecimalType(18, 2))).otherwise(
        F.col(col_name).cast(T.DecimalType(18, 2))
    )


def upper_trim(col_name):
    """Standardise categorical codes to UPPER, trimmed."""
    return F.upper(F.trim(F.col(col_name)))


def write_silver(df, table_name, partition_cols=None):
    path = f"{SILVER_PATH}/{table_name}"
    if MODE == "databricks":
        w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_cols:
            w = w.partitionBy(*partition_cols)
        w.save(path)
        # Register in Unity Catalog / Hive metastore
        spark.sql(f"CREATE TABLE IF NOT EXISTS silver.{table_name} USING DELTA LOCATION '{path}'")
    else:
        df.coalesce(1).write.mode("overwrite").parquet(path)
    count = df.count()
    print(f"  [silver] {table_name}: {count:,} rows -> {path}")
    return count


# COMMAND ----------
# %md
# ## 3. Customers — deduplicate, standardise names, age band

raw_customers = spark.read.csv(f"{BRONZE_PATH}/customers.csv", header=True, inferSchema=True)

customers_silver = (
    raw_customers
    # Remove exact duplicates (same customer_id emitted twice)
    .dropDuplicates(["customer_id"])
    # Standardise text fields
    .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
    .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
    # Normalise date of birth
    .withColumn("date_of_birth", coerce_date("date_of_birth"))
    # Age band from DOB
    .withColumn(
        "age_band",
        F.when(F.col("date_of_birth").isNull(), "Unknown")
        .when(F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25).between(18, 24), "18-24")
        .when(F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25).between(25, 34), "25-34")
        .when(F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25).between(35, 44), "35-44")
        .when(F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25).between(45, 54), "45-54")
        .otherwise("55+")
    )
    # Standardise gender
    .withColumn(
        "gender_std",
        F.when(upper_trim("gender").isin("M", "MALE"), "Male")
        .when(upper_trim("gender").isin("F", "FEMALE"), "Female")
        .when(upper_trim("gender").isin("NON-BINARY"), "Non-binary")
        .otherwise("Unknown")
    )
    # Standardise employment status
    .withColumn("employment_status_std", upper_trim("employment_status"))
    # Clean net income
    .withColumn("net_income_zar", clean_amount("net_income_zar"))
    # Data quality flags
    .withColumn("dq_missing_dob", F.col("date_of_birth").isNull().cast(T.IntegerType()))
    .withColumn("dq_missing_income", F.col("net_income_zar").isNull().cast(T.IntegerType()))
    .withColumn("dq_unverified", (F.col("is_verified") == 0).cast(T.IntegerType()))
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

write_silver(customers_silver, "customers", partition_cols=["province"])

# COMMAND ----------
# %md
# ## 4. Campaigns

raw_campaigns = spark.read.csv(f"{BRONZE_PATH}/campaigns.csv", header=True, inferSchema=True)

campaigns_silver = (
    raw_campaigns
    .dropDuplicates(["campaign_id"])
    .withColumn("start_date", coerce_date("start_date"))
    .withColumn("end_date", coerce_date("end_date"))
    .withColumn("budget_zar", clean_amount("budget_zar"))
    .withColumn("spend_zar", clean_amount("spend_zar"))
    .withColumn("ctr_pct",
        F.when(F.col("impressions") > 0,
               F.round(F.col("clicks") * 100.0 / F.col("impressions"), 4))
        .otherwise(F.lit(None))
    )
    .withColumn("channel_name_std", upper_trim("channel_name"))
    .withColumn("brand_name_std", upper_trim("brand_name"))
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

write_silver(campaigns_silver, "campaigns")

# COMMAND ----------
# %md
# ## 5. Web Sessions — remove bot traffic, standardise device type

raw_sessions = spark.read.csv(f"{BRONZE_PATH}/web_sessions.csv", header=True, inferSchema=True)

sessions_silver = (
    raw_sessions
    # Remove exact duplicates
    .dropDuplicates(["session_id"])
    # Filter out bot traffic
    .filter((F.col("is_bot") == 0) | F.col("is_bot").isNull())
    # Standardise device type
    .withColumn("device_type_std", upper_trim("device_type"))
    .withColumn("session_started_at", coerce_timestamp("session_started_at"))
    .withColumn("brand_name_std", upper_trim("brand_name"))
    # Clean session duration (cap at 3 hours = 10800 sec)
    .withColumn(
        "session_duration_seconds",
        F.when(F.col("session_duration_seconds") > 10800, F.lit(None))
        .otherwise(F.col("session_duration_seconds"))
    )
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

write_silver(sessions_silver, "web_sessions", partition_cols=["brand_name_std"])

# COMMAND ----------
# %md
# ## 6. Web Events — deduplicate, standardise event names, filter negatives

raw_events = spark.read.csv(f"{BRONZE_PATH}/web_events.csv", header=True, inferSchema=True)

# Dedup within session+event_name+event_at (30-second window)
events_with_ts = raw_events.withColumn("event_at_ts", coerce_timestamp("event_at"))

w_dedup = Window.partitionBy(
    "session_id", F.lower(F.col("event_name")),
    F.date_trunc("minute", F.col("event_at_ts"))
).orderBy("event_at_ts")

events_silver = (
    events_with_ts
    .dropDuplicates(["event_id"])
    .withColumn("event_name_std", F.lower(F.trim(F.col("event_name"))))
    .withColumn("event_value_zar", clean_amount("event_value_zar"))
    .withColumn("event_date", F.to_date(F.col("event_at_ts")))
    .withColumn("_silver_loaded_at", F.current_timestamp())
    .drop("event_at")
    .withColumnRenamed("event_at_ts", "event_at")
)

write_silver(events_silver, "web_events", partition_cols=["event_date"])

# COMMAND ----------
# %md
# ## 7. Loan Applications — deduplicate, standardise status

raw_apps = spark.read.csv(f"{BRONZE_PATH}/loan_applications.csv", header=True, inferSchema=True)

applications_silver = (
    raw_apps
    .dropDuplicates(["application_id"])
    .withColumn("application_submitted_at", coerce_timestamp("application_submitted_at"))
    .withColumn("application_status_std", upper_trim("application_status"))
    # Map dirty status variants to canonical values
    .withColumn(
        "application_status_canonical",
        F.when(upper_trim("application_status").isin("STARTED"), "STARTED")
        .when(upper_trim("application_status").isin("SUBMITTED"), "SUBMITTED")
        .when(upper_trim("application_status").isin("APPROVED"), "APPROVED")
        .when(upper_trim("application_status").isin("DECLINED"), "DECLINED")
        .when(upper_trim("application_status").isin("WITHDRAWN"), "WITHDRAWN")
        .when(upper_trim("application_status").isin("DISBURSED"), "DISBURSED")
        .otherwise("UNKNOWN")
    )
    .withColumn("channel_name_std", upper_trim("channel_name"))
    .withColumn("application_date", F.to_date(F.col("application_submitted_at")))
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

write_silver(applications_silver, "loan_applications", partition_cols=["application_date"])

# COMMAND ----------
# %md
# ## 8. Credit Checks — standardise risk bands, clean scores

raw_checks = spark.read.csv(f"{BRONZE_PATH}/credit_checks.csv", header=True, inferSchema=True)

credit_checks_silver = (
    raw_checks
    .dropDuplicates(["credit_check_id"])
    .withColumn("checked_at", coerce_timestamp("checked_at"))
    # Standardise risk band
    .withColumn(
        "risk_band_std",
        F.when(upper_trim("risk_band").isin("LOW"), "LOW")
        .when(upper_trim("risk_band").isin("MEDIUM"), "MEDIUM")
        .when(upper_trim("risk_band").isin("HIGH"), "HIGH")
        .when(upper_trim("risk_band").isin("VERY_HIGH"), "VERY_HIGH")
        .otherwise("UNKNOWN")
    )
    # Cap credit score within valid range 300-850
    .withColumn(
        "credit_score_clean",
        F.when(F.col("credit_score").between(300, 850), F.col("credit_score"))
        .otherwise(F.lit(None))
    )
    # Standardise enquiry result
    .withColumn("enquiry_result_std", upper_trim("enquiry_result"))
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

write_silver(credit_checks_silver, "credit_checks")

# COMMAND ----------
# %md
# ## 9. Affordability Assessments — clean negative amounts

raw_afford = spark.read.csv(f"{BRONZE_PATH}/affordability_assessments.csv", header=True, inferSchema=True)

affordability_silver = (
    raw_afford
    .dropDuplicates(["assessment_id"])
    .withColumn("assessed_at", coerce_timestamp("assessed_at"))
    .withColumn("net_income_zar", clean_amount("net_income_zar"))
    .withColumn("monthly_expenses_zar", clean_amount("monthly_expenses_zar"))
    .withColumn("existing_debt_zar", clean_amount("existing_debt_zar"))
    # Recalculate affordability where negative (data error)
    .withColumn(
        "affordability_amount_zar",
        F.when(
            F.col("affordability_amount_zar").isNull() |
            (F.col("net_income_zar").isNotNull() &
             F.col("monthly_expenses_zar").isNotNull() &
             F.col("existing_debt_zar").isNotNull()),
            F.col("net_income_zar") - F.col("monthly_expenses_zar") - F.col("existing_debt_zar")
        ).otherwise(F.col("affordability_amount_zar"))
    )
    # DSR ratio — recalculate if NULL
    .withColumn(
        "dsr_ratio_clean",
        F.when(
            F.col("net_income_zar") > 0,
            F.round(F.col("existing_debt_zar") / F.col("net_income_zar"), 4)
        ).otherwise(F.lit(None))
    )
    .withColumn("assessment_result_std", upper_trim("assessment_result"))
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

write_silver(affordability_silver, "affordability_assessments")

# COMMAND ----------
# %md
# ## 10. Loan Contracts — standardise status, validate dates

raw_contracts = spark.read.csv(f"{BRONZE_PATH}/loan_contracts.csv", header=True, inferSchema=True)

contracts_silver = (
    raw_contracts
    .dropDuplicates(["loan_id"])
    .dropDuplicates(["contract_number"])
    .withColumn("disbursement_date", coerce_date("disbursement_date"))
    .withColumn("principal_amount_zar", clean_amount("principal_amount_zar"))
    .withColumn("disbursed_amount_zar", clean_amount("disbursed_amount_zar"))
    .withColumn("total_repayable_zar", clean_amount("total_repayable_zar"))
    .withColumn(
        "loan_status_std",
        F.when(upper_trim("loan_status").isin("ACTIVE"), "ACTIVE")
        .when(upper_trim("loan_status").isin("SETTLED"), "SETTLED")
        .when(upper_trim("loan_status").isin("ARREARS"), "ARREARS")
        .when(upper_trim("loan_status").isin("WRITTEN_OFF"), "WRITTEN_OFF")
        .when(upper_trim("loan_status").isin("CANCELLED"), "CANCELLED")
        .otherwise("UNKNOWN")
    )
    .withColumn("disbursement_year_month", F.date_format(F.col("disbursement_date"), "yyyy-MM"))
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

write_silver(contracts_silver, "loan_contracts", partition_cols=["disbursement_year_month"])

# COMMAND ----------
# %md
# ## 11. Repayment Schedule — standardise status, clean amounts

raw_sched = spark.read.csv(f"{BRONZE_PATH}/repayment_schedule.csv", header=True, inferSchema=True)

schedule_silver = (
    raw_sched
    .dropDuplicates(["schedule_id"])
    .withColumn("due_date", coerce_date("due_date"))
    .withColumn("instalment_amount_zar", clean_amount("instalment_amount_zar"))
    .withColumn("principal_component_zar", clean_amount("principal_component_zar"))
    .withColumn("fee_interest_component_zar", clean_amount("fee_interest_component_zar"))
    .withColumn(
        "schedule_status_std",
        F.when(upper_trim("schedule_status").isin("PENDING"), "PENDING")
        .when(upper_trim("schedule_status").isin("PAID"), "PAID")
        .when(upper_trim("schedule_status").isin("PART_PAID"), "PART_PAID")
        .when(upper_trim("schedule_status").isin("OVERDUE"), "OVERDUE")
        .otherwise("UNKNOWN")
    )
    .withColumn("due_year_month", F.date_format(F.col("due_date"), "yyyy-MM"))
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

write_silver(schedule_silver, "repayment_schedule", partition_cols=["due_year_month"])

# COMMAND ----------
# %md
# ## 12. Payments — remove future dates, deduplicate, clean amounts

raw_payments = spark.read.csv(f"{BRONZE_PATH}/payments.csv", header=True, inferSchema=True)

today = F.current_date()

payments_silver = (
    raw_payments
    .dropDuplicates(["payment_id"])
    .withColumn("payment_date", coerce_date("payment_date"))
    # Flag future payment dates as dirty
    .withColumn("dq_future_payment", (F.col("payment_date") > today).cast(T.IntegerType()))
    # Null out future dates (cannot have been paid yet)
    .withColumn("payment_date_clean",
        F.when(F.col("payment_date") > today, F.lit(None)).otherwise(F.col("payment_date"))
    )
    .withColumn("payment_amount_zar", clean_amount("payment_amount_zar"))
    .withColumn("payment_method_std", F.initcap(F.trim(F.col("payment_method"))))
    .withColumn(
        "payment_status_std",
        F.when(upper_trim("payment_status").isin("SUCCESS"), "SUCCESS")
        .when(upper_trim("payment_status").isin("FAILED"), "FAILED")
        .when(upper_trim("payment_status").isin("REVERSED"), "REVERSED")
        .when(upper_trim("payment_status").isin("PENDING"), "PENDING")
        .otherwise("UNKNOWN")
    )
    .withColumn("payment_year_month", F.date_format(F.col("payment_date_clean"), "yyyy-MM"))
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

write_silver(payments_silver, "payments", partition_cols=["payment_year_month"])

# COMMAND ----------
# %md
# ## 13. Support Tickets

raw_tickets = spark.read.csv(f"{BRONZE_PATH}/support_tickets.csv", header=True, inferSchema=True)

tickets_silver = (
    raw_tickets
    .dropDuplicates(["ticket_id"])
    .withColumn("opened_at", coerce_timestamp("opened_at"))
    .withColumn("closed_at", coerce_timestamp("closed_at"))
    .withColumn(
        "ticket_status_std",
        F.when(upper_trim("ticket_status").isin("OPEN"), "OPEN")
        .when(upper_trim("ticket_status").isin("PENDING_CUSTOMER"), "PENDING_CUSTOMER")
        .when(upper_trim("ticket_status").isin("RESOLVED"), "RESOLVED")
        .when(upper_trim("ticket_status").isin("CLOSED"), "CLOSED")
        .otherwise("UNKNOWN")
    )
    .withColumn("priority_std", upper_trim("priority"))
    .withColumn("brand_name_std", upper_trim("brand_name"))
    # Resolution time in minutes
    .withColumn(
        "resolution_minutes",
        F.when(
            F.col("closed_at").isNotNull() & F.col("opened_at").isNotNull(),
            (F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60
        ).otherwise(F.lit(None))
    )
    .withColumn("ticket_date", F.to_date(F.col("opened_at")))
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

write_silver(tickets_silver, "support_tickets", partition_cols=["ticket_date"])

# COMMAND ----------
# %md
# ## 14. Reviews — standardise ratings, clean sentiment

raw_reviews = spark.read.csv(f"{BRONZE_PATH}/reviews.csv", header=True, inferSchema=True)

reviews_silver = (
    raw_reviews
    .dropDuplicates(["review_id"])
    .withColumn("review_date", coerce_date("review_date"))
    .withColumn("brand_name_std", upper_trim("brand_name"))
    # Cap rating 1.0-5.0
    .withColumn(
        "rating_clean",
        F.when(F.col("rating").between(1.0, 5.0), F.col("rating")).otherwise(F.lit(None))
    )
    # Derive sentiment from rating where label is missing
    .withColumn(
        "sentiment_std",
        F.when(F.col("sentiment_label").isNotNull(), F.initcap(F.trim(F.col("sentiment_label"))))
        .when(F.col("rating_clean") >= 4.0, "Positive")
        .when(F.col("rating_clean") <= 2.0, "Negative")
        .otherwise("Neutral")
    )
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

write_silver(reviews_silver, "reviews")

# COMMAND ----------
# %md
# ## 15. Collection Actions

raw_collections = spark.read.csv(f"{BRONZE_PATH}/collection_actions.csv", header=True, inferSchema=True)

collections_silver = (
    raw_collections
    .dropDuplicates(["collection_action_id"])
    .withColumn("action_date", coerce_date("action_date"))
    .withColumn("promise_to_pay_date", coerce_date("promise_to_pay_date"))
    .withColumn("promise_amount_zar", clean_amount("promise_amount_zar"))
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

write_silver(collections_silver, "collection_actions")

# COMMAND ----------
# %md
# ## 16. Partner Referrals

raw_referrals = spark.read.csv(f"{BRONZE_PATH}/partner_referrals.csv", header=True, inferSchema=True)

referrals_silver = (
    raw_referrals
    .dropDuplicates(["referral_id"])
    .withColumn("referred_at", coerce_timestamp("referred_at"))
    .withColumn("referral_status_std", upper_trim("referral_status"))
    .withColumn("commission_zar", clean_amount("commission_zar"))
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

write_silver(referrals_silver, "partner_referrals")

# COMMAND ----------
# %md
# ## 17. Silver Quality Report

quality_checks = [
    ("customers",           customers_silver.filter(F.col("dq_missing_dob") == 1).count(),    "Missing date_of_birth"),
    ("customers",           customers_silver.filter(F.col("dq_missing_income") == 1).count(), "Missing net_income_zar"),
    ("web_sessions",        raw_sessions.filter(F.col("is_bot") == 1).count(),                 "Bot sessions removed"),
    ("payments",            payments_silver.filter(F.col("dq_future_payment") == 1).count(),  "Future payment dates nulled"),
    ("credit_checks",       credit_checks_silver.filter(F.col("risk_band_std") == "UNKNOWN").count(), "Unknown risk band"),
    ("loan_applications",   applications_silver.filter(F.col("application_status_canonical") == "UNKNOWN").count(), "Unknown app status"),
]

print("\n=== Silver Data Quality Report ===")
for table, count, label in quality_checks:
    print(f"  {table:<30} {label:<35} {count:>6,} rows")

print("\nSilver layer complete.")
