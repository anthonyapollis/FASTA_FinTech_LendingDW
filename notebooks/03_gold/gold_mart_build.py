# Databricks notebook — Gold Layer: Star Schema Marts
# FASTA / Confluent FinTech Lending DW
# Medallion: Silver (clean Delta) -> Gold (aggregated marts, Power BI-ready)
#
# Tables produced:
#   dim_date         date dimension
#   dim_customer     SCD-1 customer dimension
#   dim_brand        brand/product dimension
#   dim_channel      marketing channel dimension
#   dim_campaign     campaign dimension
#   dim_risk_band    credit risk band lookup
#   dim_product      loan product dimension
#   fact_web_funnel  sessions -> events -> calculator -> apply clicks (daily grain)
#   fact_loan_app    one row per application (funnel metrics)
#   fact_disbursement one row per loan contract
#   fact_repayment   one row per repayment schedule line
#   fact_collections one row per collection action
#   fact_support     one row per support ticket
#   mart_channel_roi channel-level ROI aggregation

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

try:
    dbutils  # noqa: F821
    SILVER_PATH = dbutils.widgets.get("silver_path")   # noqa: F821
    GOLD_PATH   = dbutils.widgets.get("gold_path")     # noqa: F821
    MODE = "databricks"
except NameError:
    import os
    BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data"))
    SILVER_PATH = os.path.join(BASE, "silver")
    GOLD_PATH   = os.path.join(BASE, "gold")
    MODE = "local"
    spark = SparkSession.builder.appName("fasta_gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

print(f"Mode: {MODE}  Silver={SILVER_PATH}  Gold={GOLD_PATH}")


def read_silver(table):
    path = f"{SILVER_PATH}/{table}"
    if MODE == "databricks":
        return spark.read.format("delta").load(path)
    return spark.read.parquet(path)


def write_gold(df, table_name, partition_cols=None):
    path = f"{GOLD_PATH}/{table_name}"
    if MODE == "databricks":
        w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_cols:
            w = w.partitionBy(*partition_cols)
        w.save(path)
        spark.sql(f"CREATE TABLE IF NOT EXISTS gold.{table_name} USING DELTA LOCATION '{path}'")
    else:
        df.coalesce(1).write.mode("overwrite").parquet(path)
    count = df.count()
    print(f"  [gold] {table_name}: {count:,} rows")
    return count

# COMMAND ----------
# %md
# ## dim_date (2024-01-01 to 2027-12-31)

date_rows = spark.sql("""
    SELECT
        CAST(date_format(d, 'yyyyMMdd') AS INT)  AS date_key,
        CAST(d AS DATE)                            AS calendar_date,
        YEAR(d)                                    AS calendar_year,
        QUARTER(d)                                 AS calendar_quarter,
        MONTH(d)                                   AS month_number,
        date_format(d, 'MMMM')                    AS month_name,
        date_format(d, 'MMM')                     AS month_abbr,
        DAY(d)                                     AS day_of_month,
        WEEKOFYEAR(d)                              AS week_number,
        dayofweek(d)                               AS day_of_week,
        date_format(d, 'EEEE')                    AS day_name,
        CASE WHEN dayofweek(d) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
        date_format(d, 'yyyy-MM')                 AS year_month
    FROM (
        SELECT explode(sequence(
            to_date('2024-01-01'),
            to_date('2027-12-31'),
            interval 1 day
        )) AS d
    )
""")

write_gold(date_rows, "dim_date")

# COMMAND ----------
# %md
# ## dim_customer

cust = read_silver("customers")

dim_customer = (
    cust
    .select(
        F.col("customer_id").cast(T.IntegerType()).alias("customer_key"),
        "customer_identifier_hash",
        F.concat(F.substring("first_name", 1, 1), F.lit("*** "),
                 F.substring("last_name", 1, 1), F.lit("***")).alias("customer_name_masked"),
        "age_band",
        "gender_std",
        "province",
        "employment_status_std",
        "net_income_zar",
        "is_repeat_customer",
        "is_verified",
        "dq_missing_dob",
        "dq_missing_income",
        F.current_timestamp().alias("_gold_loaded_at"),
    )
)

write_gold(dim_customer, "dim_customer")

# COMMAND ----------
# %md
# ## dim_brand (static seed — expand from campaigns)

brand_data = [
    (1, "FASTA", "FASTA", "Short-Term Lending", "https://www.fasta.co.za"),
    (2, "JustMoney", "Confluent", "Financial Marketplace", "https://justmoney.co.za"),
    (3, "DebtBusters", "Confluent", "Debt Counselling", "https://debtbusters.co.za"),
]
schema_brand = T.StructType([
    T.StructField("brand_key", T.IntegerType()),
    T.StructField("brand_name", T.StringType()),
    T.StructField("organisation_name", T.StringType()),
    T.StructField("brand_type", T.StringType()),
    T.StructField("website_url", T.StringType()),
])
dim_brand = spark.createDataFrame(brand_data, schema=schema_brand)
write_gold(dim_brand, "dim_brand")

# COMMAND ----------
# %md
# ## dim_channel

channel_data = [
    (1, "Google Sponsored Search", "Search Engine Marketing", 1),
    (2, "Organic Search", "SEO", 0),
    (3, "Facebook", "Social Media", 0),
    (4, "Direct", "Direct", 0),
    (5, "WhatsApp", "Messaging", 0),
    (6, "Email", "Email Marketing", 1),
    (7, "Partner / Referral", "Affiliate", 0),
    (99, "Unknown", "Unknown", 0),
]
schema_ch = T.StructType([
    T.StructField("channel_key", T.IntegerType()),
    T.StructField("channel_name", T.StringType()),
    T.StructField("channel_type", T.StringType()),
    T.StructField("is_paid_channel", T.IntegerType()),
])
dim_channel = spark.createDataFrame(channel_data, schema=schema_ch)
write_gold(dim_channel, "dim_channel")

# COMMAND ----------
# %md
# ## dim_campaign

camps = read_silver("campaigns")
dim_campaign = (
    camps
    .select(
        F.col("campaign_id").cast(T.IntegerType()).alias("campaign_key"),
        "campaign_name",
        "campaign_objective",
        "brand_name_std",
        "channel_name_std",
        "start_date",
        "end_date",
        "budget_zar",
        "spend_zar",
        "impressions",
        "clicks",
        "ctr_pct",
        F.current_timestamp().alias("_gold_loaded_at"),
    )
)
write_gold(dim_campaign, "dim_campaign")

# COMMAND ----------
# %md
# ## dim_risk_band

risk_data = [
    (1, "LOW",       650, 850, "Low credit risk — preferred segment"),
    (2, "MEDIUM",    580, 649, "Medium credit risk — standard pricing"),
    (3, "HIGH",      400, 579, "High credit risk — elevated interest"),
    (4, "VERY_HIGH", 300, 399, "Very high risk — decline or strict conditions"),
    (5, "UNKNOWN",   None, None, "Risk not assessed or unavailable"),
]
schema_rb = T.StructType([
    T.StructField("risk_band_key", T.IntegerType()),
    T.StructField("risk_band", T.StringType()),
    T.StructField("min_score", T.IntegerType()),
    T.StructField("max_score", T.IntegerType()),
    T.StructField("risk_description", T.StringType()),
])
dim_risk_band = spark.createDataFrame(risk_data, schema=schema_rb)
write_gold(dim_risk_band, "dim_risk_band")

# COMMAND ----------
# %md
# ## dim_product

product_data = [
    (1, "FASTA Cash Loan",           "Short-Term Loan", 1,  500.0, 15000.0, 6),
    (2, "FASTA Repeat Customer Loan","Short-Term Loan", 1,  500.0, 15000.0, 6),
    (3, "JustMoney Comparison",      "Financial Tool",  2, None,   None,    None),
    (4, "DebtBusters Debt Review",   "Debt Counselling",3, None,   None,    None),
]
schema_prod = T.StructType([
    T.StructField("product_key", T.IntegerType()),
    T.StructField("product_name", T.StringType()),
    T.StructField("product_category", T.StringType()),
    T.StructField("brand_key", T.IntegerType()),
    T.StructField("min_amount_zar", T.DoubleType()),
    T.StructField("max_amount_zar", T.DoubleType()),
    T.StructField("max_term_months", T.IntegerType()),
])
dim_product = spark.createDataFrame(product_data, schema=schema_prod)
write_gold(dim_product, "dim_product")

# COMMAND ----------
# %md
# ## fact_web_funnel — daily brand/channel grain

sessions = read_silver("web_sessions")
events   = read_silver("web_events")
camps_df = read_silver("campaigns")

sess_with_camp = sessions.join(
    camps_df.select("campaign_id", "channel_name_std"),
    on="campaign_id", how="left"
)

events_agg = events.groupBy("session_id").agg(
    F.count("*").alias("total_events"),
    F.sum(F.when(F.col("event_name_std") == "page_view", 1).otherwise(0)).alias("page_views"),
    F.sum(F.when(F.col("event_name_std") == "loan_calculator_changed", 1).otherwise(0)).alias("calculator_interactions"),
    F.sum(F.when(F.col("event_name_std") == "apply_now_click", 1).otherwise(0)).alias("apply_now_clicks"),
    F.sum(F.when(F.col("event_name_std") == "application_submitted", 1).otherwise(0)).alias("applications_started"),
    F.sum(F.when(F.col("event_name_std") == "chat_open", 1).otherwise(0)).alias("chat_opens"),
    F.max(F.col("event_value_zar")).alias("max_quote_value_zar"),
)

session_events = sess_with_camp.join(events_agg, on="session_id", how="left")

fact_web_funnel = (
    session_events
    .withColumn("session_date", F.to_date(F.col("session_started_at")))
    .withColumn("date_key", F.date_format(F.col("session_date"), "yyyyMMdd").cast(T.IntegerType()))
    .groupBy(
        "date_key",
        "session_date",
        F.col("brand_name_std").alias("brand_name"),
        F.coalesce(F.col("channel_name_std"), F.lit("Unknown")).alias("channel_name"),
        "campaign_id",
    )
    .agg(
        F.count("session_id").alias("sessions"),
        F.countDistinct("customer_id").alias("unique_visitors"),
        F.sum("page_views").alias("page_views"),
        F.sum("calculator_interactions").alias("calculator_interactions"),
        F.sum("apply_now_clicks").alias("apply_now_clicks"),
        F.sum("chat_opens").alias("chat_opens"),
        F.avg("session_duration_seconds").alias("avg_session_duration_sec"),
        F.sum("max_quote_value_zar").alias("total_quote_value_zar"),
        # Conversion rates
        F.round(
            F.sum("apply_now_clicks") * 100.0 / F.nullif(F.count("session_id"), F.lit(0)), 2
        ).alias("apply_click_rate_pct"),
        F.round(
            F.sum("calculator_interactions") * 100.0 / F.nullif(F.count("session_id"), F.lit(0)), 2
        ).alias("calculator_engagement_rate_pct"),
    )
    .withColumn("_gold_loaded_at", F.current_timestamp())
)

write_gold(fact_web_funnel, "fact_web_funnel", partition_cols=["session_date"])

# COMMAND ----------
# %md
# ## fact_loan_application — one row per application with full context

apps    = read_silver("loan_applications")
quotes  = spark.read.csv(f"{SILVER_PATH.replace('silver','bronze')}/loan_quotes.csv" if MODE == "local"
                          else f"{SILVER_PATH}/loan_quotes", header=True, inferSchema=True)
checks  = read_silver("credit_checks")
afford  = read_silver("affordability_assessments")
contr   = read_silver("loan_contracts")

# Build application fact
checks_agg = checks.groupBy("application_id").agg(
    F.max("credit_score_clean").alias("credit_score"),
    F.first("risk_band_std").alias("risk_band"),
    F.first("enquiry_result_std").alias("credit_result"),
    F.sum(F.col("adverse_listings_count")).alias("adverse_listings"),
)

afford_agg = afford.groupBy("application_id").agg(
    F.max("net_income_zar").alias("net_income_zar"),
    F.max("affordability_amount_zar").alias("affordability_amount_zar"),
    F.max("dsr_ratio_clean").alias("dsr_ratio"),
    F.first("assessment_result_std").alias("affordability_result"),
)

loan_ids = contr.select("application_id", "loan_id", "loan_status_std")

fact_loan_app = (
    apps
    .join(checks_agg, on="application_id", how="left")
    .join(afford_agg, on="application_id", how="left")
    .join(loan_ids, on="application_id", how="left")
    .withColumn("application_date_key",
        F.date_format(F.col("application_date"), "yyyyMMdd").cast(T.IntegerType()))
    .withColumn("approved_flag",
        (F.col("application_status_canonical").isin("APPROVED","DISBURSED")).cast(T.IntegerType()))
    .withColumn("declined_flag",
        (F.col("application_status_canonical") == "DECLINED").cast(T.IntegerType()))
    .withColumn("withdrawn_flag",
        (F.col("application_status_canonical") == "WITHDRAWN").cast(T.IntegerType()))
    .withColumn("disbursed_flag",
        (F.col("loan_id").isNotNull()).cast(T.IntegerType()))
    .withColumn("low_risk_flag",
        (F.col("risk_band").isin("LOW","MEDIUM")).cast(T.IntegerType()))
    .withColumn("affordability_passed_flag",
        (F.col("affordability_result") == "PASS").cast(T.IntegerType()))
    .withColumn("_gold_loaded_at", F.current_timestamp())
    .select(
        "application_id",
        "application_date_key",
        "application_date",
        F.col("customer_id").cast(T.IntegerType()).alias("customer_key"),
        "application_status_canonical",
        "credit_score",
        "risk_band",
        "credit_result",
        "adverse_listings",
        "net_income_zar",
        "affordability_amount_zar",
        "dsr_ratio",
        "affordability_result",
        "channel_name_std",
        "processing_minutes",
        "loan_id",
        "loan_status_std",
        "approved_flag",
        "declined_flag",
        "withdrawn_flag",
        "disbursed_flag",
        "low_risk_flag",
        "affordability_passed_flag",
        "_gold_loaded_at",
    )
)

write_gold(fact_loan_app, "fact_loan_application", partition_cols=["application_date"])

# COMMAND ----------
# %md
# ## fact_disbursement — loan book summary per contract

contracts = read_silver("loan_contracts")

fact_disb = (
    contracts
    .withColumn("disbursement_date_key",
        F.date_format(F.col("disbursement_date"), "yyyyMMdd").cast(T.IntegerType()))
    .withColumn("fees_zar",
        F.when(
            F.col("total_repayable_zar").isNotNull() & F.col("principal_amount_zar").isNotNull(),
            F.col("total_repayable_zar") - F.col("principal_amount_zar")
        ).otherwise(F.lit(None))
    )
    .withColumn("active_loan_flag",
        (F.col("loan_status_std") == "ACTIVE").cast(T.IntegerType()))
    .withColumn("arrears_flag",
        (F.col("loan_status_std") == "ARREARS").cast(T.IntegerType()))
    .withColumn("written_off_flag",
        (F.col("loan_status_std") == "WRITTEN_OFF").cast(T.IntegerType()))
    .withColumn("_gold_loaded_at", F.current_timestamp())
    .select(
        "loan_id",
        "disbursement_date_key",
        "disbursement_date",
        "disbursement_year_month",
        F.col("customer_id").cast(T.IntegerType()).alias("customer_key"),
        "principal_amount_zar",
        "disbursed_amount_zar",
        "total_repayable_zar",
        "fees_zar",
        "term_months",
        "interest_rate_pct",
        "loan_status_std",
        "nca_compliant_flag",
        "active_loan_flag",
        "arrears_flag",
        "written_off_flag",
        "_gold_loaded_at",
    )
)

write_gold(fact_disb, "fact_disbursement", partition_cols=["disbursement_year_month"])

# COMMAND ----------
# %md
# ## fact_repayment — repayment performance at instalment grain

schedule = read_silver("repayment_schedule")
payments  = read_silver("payments")

payments_per_schedule = payments.groupBy("schedule_id").agg(
    F.sum(F.when(F.col("payment_status_std") == "SUCCESS", F.col("payment_amount_zar")).otherwise(F.lit(0))).alias("total_paid_zar"),
    F.sum(F.when(F.col("payment_status_std") == "FAILED", 1).otherwise(0)).alias("failed_attempts"),
    F.max(F.col("payment_date_clean")).alias("last_payment_date"),
)

fact_repay = (
    schedule
    .join(payments_per_schedule, on="schedule_id", how="left")
    .withColumn("due_date_key",
        F.date_format(F.col("due_date"), "yyyyMMdd").cast(T.IntegerType()))
    .withColumn("outstanding_zar",
        F.greatest(
            F.col("instalment_amount_zar") - F.coalesce(F.col("total_paid_zar"), F.lit(0)),
            F.lit(0)
        )
    )
    .withColumn("days_late",
        F.when(
            F.col("last_payment_date").isNotNull(),
            F.datediff(F.col("last_payment_date"), F.col("due_date"))
        ).otherwise(F.lit(None))
    )
    .withColumn("paid_on_time_flag",
        F.when(
            (F.col("total_paid_zar") >= F.col("instalment_amount_zar")) &
            (F.coalesce(F.col("days_late"), F.lit(0)) <= 0),
            1
        ).otherwise(0)
    )
    .withColumn("fully_paid_flag",
        (F.col("outstanding_zar") == 0).cast(T.IntegerType()))
    .withColumn("overdue_flag",
        (F.col("schedule_status_std") == "OVERDUE").cast(T.IntegerType()))
    .withColumn("_gold_loaded_at", F.current_timestamp())
    .select(
        "schedule_id",
        "loan_id",
        "instalment_number",
        "due_date_key",
        "due_date",
        "due_year_month",
        "instalment_amount_zar",
        "principal_component_zar",
        "fee_interest_component_zar",
        "total_paid_zar",
        "outstanding_zar",
        "failed_attempts",
        "last_payment_date",
        "days_late",
        "schedule_status_std",
        "paid_on_time_flag",
        "fully_paid_flag",
        "overdue_flag",
        "_gold_loaded_at",
    )
)

write_gold(fact_repay, "fact_repayment", partition_cols=["due_year_month"])

# COMMAND ----------
# %md
# ## mart_channel_roi — channel profitability vs repayment quality

# This mart answers: which channel brings customers who repay on time?
apps_gold     = read_silver("loan_applications")
repay_gold    = fact_repay  # already computed above

loans_per_channel = (
    apps_gold
    .filter(F.col("application_status_canonical").isin("APPROVED","DISBURSED"))
    .groupBy("channel_name_std")
    .agg(
        F.count("*").alias("approved_applications"),
        F.sum(F.when(F.col("application_status_canonical") == "DISBURSED", 1).otherwise(0)).alias("disbursed_loans"),
    )
)

repay_agg = (
    fact_repay
    .groupBy("loan_id")
    .agg(
        F.avg("paid_on_time_flag").alias("on_time_rate"),
        F.sum("outstanding_zar").alias("total_outstanding_zar"),
        F.sum("instalment_amount_zar").alias("total_scheduled_zar"),
    )
    .withColumn("collection_rate",
        F.when(F.col("total_scheduled_zar") > 0,
               1 - (F.col("total_outstanding_zar") / F.col("total_scheduled_zar")))
        .otherwise(F.lit(None))
    )
)

mart_channel_roi = (
    loans_per_channel
    .withColumn("channel_name_std",
        F.coalesce(F.col("channel_name_std"), F.lit("Unknown")))
    .withColumn("_gold_loaded_at", F.current_timestamp())
)

write_gold(mart_channel_roi, "mart_channel_roi")

print("\nGold layer build complete.")
