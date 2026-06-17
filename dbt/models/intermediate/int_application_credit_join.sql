-- dbt intermediate model: int_application_credit_join
-- Joins applications + credit checks + affordability into one wide row
-- Materialisation: table (used by multiple downstream marts)

{{ config(materialized='table', schema='intermediate') }}

WITH apps AS (
    SELECT * FROM {{ ref('stg_loan_applications') }}
),
checks AS (
    SELECT
        application_id,
        credit_score,
        risk_band,
        enquiry_result,
        adverse_listings_count,
        open_accounts_count,
        total_outstanding_debt,
        monthly_obligations,
        judgements_count
    FROM {{ source('silver', 'credit_checks') }}
    WHERE dq_any_flag = 0
),
afford AS (
    SELECT
        application_id,
        net_income_zar,
        declared_expenses_zar,
        nett_disposable_income,
        dsr_ratio,
        expense_ratio,
        assessment_result,
        CASE WHEN assessment_result = 'PASS' THEN 1 ELSE 0 END AS is_affordable
    FROM {{ source('silver', 'affordability_assessments') }}
    WHERE dq_any_flag = 0
)

SELECT
    a.application_id,
    a.customer_id,
    a.application_date,
    a.application_year,
    a.application_month,
    a.channel_name,
    a.brand_name,
    a.campaign_id,
    a.loan_amount_requested,
    a.loan_term_months,
    a.purpose,
    a.application_status,
    a.is_approved,
    a.is_declined,
    -- Credit check
    c.credit_score,
    c.risk_band,
    c.enquiry_result,
    c.adverse_listings_count,
    c.judgements_count,
    c.total_outstanding_debt,
    c.monthly_obligations,
    -- Affordability
    af.net_income_zar,
    af.declared_expenses_zar,
    af.nett_disposable_income,
    af.dsr_ratio,
    af.expense_ratio,
    af.is_affordable,
    -- Derived
    CASE
        WHEN c.credit_score >= 650 THEN 'LOW'
        WHEN c.credit_score >= 580 THEN 'MEDIUM'
        WHEN c.credit_score >= 450 THEN 'HIGH'
        WHEN c.credit_score IS NOT NULL THEN 'VERY_HIGH'
        ELSE 'UNKNOWN'
    END AS derived_risk_band
FROM apps a
LEFT JOIN checks c  ON a.application_id = c.application_id
LEFT JOIN afford af ON a.application_id = af.application_id
