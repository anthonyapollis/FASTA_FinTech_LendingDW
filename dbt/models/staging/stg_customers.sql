-- dbt staging model: stg_customers
-- Source: silver.customers  |  Materialisation: view
-- Adds consistent column aliases and casts for downstream marts

{{ config(materialized='view', schema='staging') }}

SELECT
    customer_id,
    id_number_hash,
    full_name,
    email_address,
    date_of_birth,
    age_years,
    age_band,
    COALESCE(gender, 'UNKNOWN')              AS gender,
    COALESCE(employment_status, 'UNKNOWN')   AS employment_status,
    net_income_zar,
    income_band,
    province,
    city,
    registration_date,
    kyc_status,
    -- DQ pass/fail flag for downstream filtering
    dq_any_flag,
    _silver_load_ts
FROM {{ source('silver', 'customers') }}
WHERE customer_id IS NOT NULL
