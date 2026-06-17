-- dbt mart model: mart_credit_risk
-- Joins ML scores back to application facts for Power BI credit risk reports
-- Materialisation: table (full refresh — small dataset)

{{ config(materialized='table', schema='gold') }}

WITH apps AS (
    SELECT * FROM {{ ref('int_application_credit_join') }}
),
contracts AS (
    SELECT
        application_id,
        loan_id,
        loan_status,
        principal_amount_zar,
        disbursal_date,
        CASE WHEN loan_status IN ('ARREARS','WRITTEN_OFF') THEN 1 ELSE 0 END AS is_default
    FROM {{ source('silver', 'loan_contracts') }}
),
repay AS (
    SELECT * FROM {{ ref('int_loan_repayment_stats') }}
)

SELECT
    a.application_id,
    a.customer_id,
    a.application_date,
    a.channel_name,
    a.brand_name,
    a.loan_amount_requested,
    a.loan_term_months,
    a.credit_score,
    a.risk_band,
    a.derived_risk_band,
    a.dsr_ratio,
    a.expense_ratio,
    a.net_income_zar,
    a.nett_disposable_income,
    a.adverse_listings_count,
    a.judgements_count,
    a.total_outstanding_debt,
    a.is_approved,
    a.is_affordable,
    -- Contract outcome
    c.loan_id,
    c.loan_status,
    c.principal_amount_zar,
    c.disbursal_date,
    c.is_default,
    -- Repayment performance
    r.collection_rate,
    r.payment_success_rate,
    r.days_since_last_payment,
    r.total_paid_zar,
    r.total_scheduled_zar,
    -- Risk segments for dashboard
    CASE
        WHEN c.is_default = 1                    THEN 'DEFAULTED'
        WHEN a.credit_score >= 700               THEN 'PRIME'
        WHEN a.credit_score >= 600               THEN 'NEAR_PRIME'
        ELSE                                          'SUB_PRIME'
    END AS customer_segment,
    CURRENT_TIMESTAMP                            AS _mart_built_ts
FROM apps a
LEFT JOIN contracts c ON a.application_id = c.application_id
LEFT JOIN repay r     ON c.loan_id        = r.loan_id
