-- =============================================================================
-- FASTA FinTech — MS Fabric Lakehouse DDL
-- Delta Lake table definitions for FastaLakehouse (OneLake)
-- Engine: Fabric Warehouse SQL Endpoint / Spark SQL
-- Author: Anthony Apollis | 2026-06-17
-- =============================================================================
-- Usage:
--   Run in Fabric Lakehouse SQL Endpoint (T-SQL dialect)
--   OR adapt to Spark SQL by replacing DECIMAL → DOUBLE, VARCHAR → STRING
-- =============================================================================

-- ============================================================================
-- BRONZE SCHEMA (raw ingest — partition by _ingest_date)
-- ============================================================================
-- In Fabric Lakehouse, these are Delta tables in Files/bronze/

CREATE TABLE IF NOT EXISTS bronze.customers
USING DELTA
PARTITIONED BY (_ingest_date)
LOCATION 'abfss://onelake@onelake.dfs.fabric.microsoft.com/FastaLakehouse.Lakehouse/Files/bronze/customers/'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.minReaderVersion'     = '2',
    'delta.minWriterVersion'     = '5'
) AS
SELECT
    CAST(NULL AS STRING)   AS customer_id,
    CAST(NULL AS STRING)   AS id_number_hash,
    CAST(NULL AS STRING)   AS mobile_masked,
    CAST(NULL AS STRING)   AS first_name,
    CAST(NULL AS STRING)   AS last_name,
    CAST(NULL AS STRING)   AS email_address,
    CAST(NULL AS STRING)   AS date_of_birth,
    CAST(NULL AS STRING)   AS gender,
    CAST(NULL AS STRING)   AS employment_status,
    CAST(NULL AS DOUBLE)   AS net_income_zar,
    CAST(NULL AS STRING)   AS province,
    CAST(NULL AS STRING)   AS city,
    CAST(NULL AS STRING)   AS registration_date,
    CAST(NULL AS STRING)   AS kyc_status,
    CAST(NULL AS TIMESTAMP) AS _ingest_timestamp,
    CAST(NULL AS STRING)   AS _source_file,
    CAST(NULL AS DATE)     AS _ingest_date
WHERE 1=0;

-- (Other bronze tables follow same pattern — abbreviated for brevity)
-- Full list: campaigns, web_sessions, web_events, loan_applications,
--            credit_checks, affordability_assessments, loan_contracts,
--            repayment_schedule, payments, fee_charges, collection_actions,
--            support_tickets, reviews, partner_referrals

-- ============================================================================
-- SILVER SCHEMA (cleansed — partition by _silver_load_date)
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.customers
USING DELTA
PARTITIONED BY (_silver_load_date)
LOCATION 'abfss://onelake@onelake.dfs.fabric.microsoft.com/FastaLakehouse.Lakehouse/Tables/silver/customers/'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'   = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
) AS
SELECT
    CAST(NULL AS STRING)    AS customer_id,
    CAST(NULL AS STRING)    AS id_number_hash,
    CAST(NULL AS STRING)    AS mobile_masked,
    CAST(NULL AS STRING)    AS first_name,
    CAST(NULL AS STRING)    AS last_name,
    CAST(NULL AS STRING)    AS full_name,
    CAST(NULL AS STRING)    AS email_address,
    CAST(NULL AS DATE)      AS date_of_birth,
    CAST(NULL AS INT)       AS age_years,
    CAST(NULL AS STRING)    AS age_band,
    CAST(NULL AS STRING)    AS gender,
    CAST(NULL AS STRING)    AS employment_status,
    CAST(NULL AS DOUBLE)    AS net_income_zar,
    CAST(NULL AS STRING)    AS income_band,
    CAST(NULL AS STRING)    AS province,
    CAST(NULL AS STRING)    AS city,
    CAST(NULL AS DATE)      AS registration_date,
    CAST(NULL AS STRING)    AS kyc_status,
    CAST(NULL AS INT)       AS dq_invalid_dob,
    CAST(NULL AS INT)       AS dq_invalid_income,
    CAST(NULL AS INT)       AS dq_missing_email,
    CAST(NULL AS INT)       AS dq_any_flag,
    CAST(NULL AS TIMESTAMP) AS _silver_load_ts,
    CAST(NULL AS DATE)      AS _silver_load_date
WHERE 1=0;

-- Silver payments (shows future-date handling)
CREATE TABLE IF NOT EXISTS silver.payments
USING DELTA
PARTITIONED BY (_silver_load_date)
LOCATION 'abfss://onelake@onelake.dfs.fabric.microsoft.com/FastaLakehouse.Lakehouse/Tables/silver/payments/'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'   = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
) AS
SELECT
    CAST(NULL AS STRING)    AS payment_id,
    CAST(NULL AS STRING)    AS loan_id,
    CAST(NULL AS DATE)      AS payment_date,         -- NULL when was future date
    CAST(NULL AS DOUBLE)    AS payment_amount_zar,   -- NULL when negative
    CAST(NULL AS STRING)    AS payment_method,
    CAST(NULL AS STRING)    AS payment_status,
    CAST(NULL AS STRING)    AS reference_number,
    CAST(NULL AS INT)       AS dq_future_date,
    CAST(NULL AS INT)       AS dq_negative_amount,
    CAST(NULL AS INT)       AS dq_orphan_loan,
    CAST(NULL AS INT)       AS dq_any_flag,
    CAST(NULL AS TIMESTAMP) AS _silver_load_ts,
    CAST(NULL AS DATE)      AS _silver_load_date
WHERE 1=0;

-- ============================================================================
-- GOLD SCHEMA (star schema — unpartitioned, Z-ordered)
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.dim_date
USING DELTA
LOCATION 'abfss://onelake@onelake.dfs.fabric.microsoft.com/FastaLakehouse.Lakehouse/Tables/gold/dim_date/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
) AS
SELECT
    CAST(NULL AS INT)       AS date_key,
    CAST(NULL AS DATE)      AS full_date,
    CAST(NULL AS INT)       AS day_of_week,
    CAST(NULL AS STRING)    AS day_name,
    CAST(NULL AS INT)       AS day_of_month,
    CAST(NULL AS INT)       AS week_of_year,
    CAST(NULL AS INT)       AS month_num,
    CAST(NULL AS STRING)    AS month_name,
    CAST(NULL AS STRING)    AS quarter_name,
    CAST(NULL AS INT)       AS year_num,
    CAST(NULL AS INT)       AS fiscal_year,
    CAST(NULL AS BOOLEAN)   AS is_weekend,
    CAST(NULL AS BOOLEAN)   AS is_public_holiday_za
WHERE 1=0;

CREATE TABLE IF NOT EXISTS gold.fact_loan_application
USING DELTA
LOCATION 'abfss://onelake@onelake.dfs.fabric.microsoft.com/FastaLakehouse.Lakehouse/Tables/gold/fact_loan_application/'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'   = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.dataSkippingNumIndexedCols' = '8'
) AS
SELECT
    CAST(NULL AS BIGINT)    AS application_sk,
    CAST(NULL AS STRING)    AS application_id,
    CAST(NULL AS INT)       AS date_key,
    CAST(NULL AS BIGINT)    AS customer_sk,
    CAST(NULL AS INT)       AS channel_sk,
    CAST(NULL AS INT)       AS campaign_sk,
    CAST(NULL AS INT)       AS brand_sk,
    CAST(NULL AS INT)       AS risk_band_sk,
    CAST(NULL AS DOUBLE)    AS loan_amount_requested,
    CAST(NULL AS INT)       AS loan_term_months,
    CAST(NULL AS INT)       AS credit_score,
    CAST(NULL AS DOUBLE)    AS dsr_ratio,
    CAST(NULL AS DOUBLE)    AS net_income_zar,
    CAST(NULL AS BOOLEAN)   AS is_approved,
    CAST(NULL AS BOOLEAN)   AS is_declined,
    CAST(NULL AS BOOLEAN)   AS is_funded,
    CAST(NULL AS DOUBLE)    AS nett_disposable_income,
    CAST(NULL AS BOOLEAN)   AS is_affordable
WHERE 1=0;

CREATE TABLE IF NOT EXISTS gold.mart_channel_roi
USING DELTA
LOCATION 'abfss://onelake@onelake.dfs.fabric.microsoft.com/FastaLakehouse.Lakehouse/Tables/gold/mart_channel_roi/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
) AS
SELECT
    CAST(NULL AS BIGINT)    AS roi_sk,
    CAST(NULL AS STRING)    AS year_week,
    CAST(NULL AS INT)       AS date_key,
    CAST(NULL AS INT)       AS channel_sk,
    CAST(NULL AS INT)       AS brand_sk,
    CAST(NULL AS INT)       AS sessions,
    CAST(NULL AS INT)       AS applications,
    CAST(NULL AS INT)       AS disbursements,
    CAST(NULL AS DOUBLE)    AS total_disbursed_zar,
    CAST(NULL AS DOUBLE)    AS total_collected_zar,
    CAST(NULL AS DOUBLE)    AS campaign_spend_zar,
    CAST(NULL AS DOUBLE)    AS approval_rate,
    CAST(NULL AS DOUBLE)    AS default_rate,
    CAST(NULL AS DOUBLE)    AS collection_rate,
    CAST(NULL AS DOUBLE)    AS channel_quality_score,
    CAST(NULL AS DOUBLE)    AS budget_recommendation
WHERE 1=0;

-- ============================================================================
-- ML SCORES TABLES (written by Fabric ML notebooks)
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.mart_credit_risk_scores
USING DELTA
LOCATION 'abfss://onelake@onelake.dfs.fabric.microsoft.com/FastaLakehouse.Lakehouse/Tables/gold/mart_credit_risk_scores/'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
) AS
SELECT
    CAST(NULL AS BIGINT)    AS score_sk,
    CAST(NULL AS STRING)    AS application_id,
    CAST(NULL AS STRING)    AS model_version,
    CAST(NULL AS DATE)      AS score_date,
    CAST(NULL AS DOUBLE)    AS default_probability,
    CAST(NULL AS INT)       AS risk_decile,
    CAST(NULL AS STRING)    AS risk_label,
    CAST(NULL AS STRING)    AS recommended_action,
    CAST(NULL AS DOUBLE)    AS model_auc
WHERE 1=0;

-- ============================================================================
-- OPTIMIZE + ZORDER COMMANDS (run post-initial load)
-- ============================================================================
/*
-- Run after Gold tables are first populated:
OPTIMIZE gold.fact_loan_application ZORDER BY (date_key, channel_sk);
OPTIMIZE gold.fact_repayment        ZORDER BY (date_key, loan_id);
OPTIMIZE gold.fact_disbursement     ZORDER BY (date_key, channel_sk, brand_sk);
OPTIMIZE gold.mart_channel_roi      ZORDER BY (year_week, channel_sk);
OPTIMIZE silver.payments            ZORDER BY (loan_id);
OPTIMIZE silver.customers           ZORDER BY (customer_id);

-- Vacuum stale files (retain 7-day history for time-travel)
VACUUM gold.fact_loan_application RETAIN 168 HOURS;
VACUUM silver.payments            RETAIN 168 HOURS;
*/

-- ============================================================================
-- FABRIC SHORTCUT DEFINITIONS (reference only — create via Fabric UI / REST)
-- ============================================================================
/*
Shortcuts connect external ADLS Gen2 / S3 paths into OneLake:

POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{lakhouseId}/shortcuts
{
  "path": "Files/bronze/customers",
  "name": "customers",
  "target": {
    "type": "AdlsGen2",
    "location": "https://<storage>.dfs.core.windows.net/",
    "subpath": "/landing/fasta/customers"
  }
}
*/
