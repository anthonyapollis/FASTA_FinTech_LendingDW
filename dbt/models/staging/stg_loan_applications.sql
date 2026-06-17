-- dbt staging model: stg_loan_applications
-- Source: silver.loan_applications  |  Materialisation: view

{{ config(materialized='view', schema='staging') }}

SELECT
    application_id,
    customer_id,
    application_date,
    EXTRACT(YEAR  FROM application_date)                    AS application_year,
    EXTRACT(MONTH FROM application_date)                    AS application_month,
    channel_name,
    loan_amount_requested,
    loan_term_months,
    purpose,
    UPPER(TRIM(application_status))                         AS application_status,
    brand_name,
    campaign_id,
    CASE WHEN application_status = 'APPROVED'  THEN 1 ELSE 0 END AS is_approved,
    CASE WHEN application_status = 'DECLINED'  THEN 1 ELSE 0 END AS is_declined,
    CASE WHEN application_status IN ('PENDING','REFERRED') THEN 1 ELSE 0 END AS is_pending,
    dq_any_flag,
    _silver_load_ts
FROM {{ source('silver', 'loan_applications') }}
WHERE application_id IS NOT NULL
