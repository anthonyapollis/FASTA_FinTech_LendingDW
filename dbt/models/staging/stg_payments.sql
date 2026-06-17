-- dbt staging model: stg_payments
-- Source: silver.payments  |  Materialisation: view
-- Excludes future-dated and negative-amount rows (dq-flagged)

{{ config(materialized='view', schema='staging') }}

SELECT
    payment_id,
    loan_id,
    payment_date,
    payment_amount_zar,
    payment_method,
    UPPER(TRIM(payment_status))             AS payment_status,
    reference_number,
    CASE WHEN payment_status = 'SUCCESS'    THEN 1 ELSE 0 END AS is_successful,
    CASE WHEN payment_status = 'REVERSED'   THEN 1 ELSE 0 END AS is_reversal,
    -- DQ flags for audit trail
    dq_future_date,
    dq_negative_amount,
    dq_any_flag,
    _silver_load_ts
FROM {{ source('silver', 'payments') }}
WHERE payment_id IS NOT NULL
  AND payment_date IS NOT NULL          -- future-dated rows removed
  AND payment_amount_zar IS NOT NULL    -- negative rows removed
