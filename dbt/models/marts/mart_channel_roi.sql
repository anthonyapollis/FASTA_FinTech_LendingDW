-- dbt mart model: mart_channel_roi
-- Weekly channel-level ROI with composite quality score
-- Materialisation: incremental (append new weeks)

{{
  config(
    materialized='incremental',
    schema='gold',
    unique_key='roi_sk',
    incremental_strategy='merge',
    partition_by={'field': 'year_week', 'data_type': 'string'}
  )
}}

WITH apps AS (
    SELECT * FROM {{ ref('int_application_credit_join') }}
),
contracts AS (
    SELECT
        loan_id,
        application_id,
        principal_amount_zar,
        loan_status,
        is_default
    FROM {{ source('silver', 'loan_contracts') }}
),
repay_stats AS (
    SELECT * FROM {{ ref('int_loan_repayment_stats') }}
),
campaigns AS (
    SELECT
        campaign_id,
        channel_name,
        spend_zar,
        brand_name,
        DATE_TRUNC('week', start_date) AS campaign_week
    FROM {{ source('silver', 'campaigns') }}
),
sessions AS (
    SELECT
        channel_name,
        brand_name,
        DATE_TRUNC('week', session_date) AS session_week,
        COUNT(*)                          AS sessions
    FROM {{ source('silver', 'web_sessions') }}
    GROUP BY channel_name, brand_name, DATE_TRUNC('week', session_date)
),
base AS (
    SELECT
        TO_CHAR(DATE_TRUNC('week', a.application_date), 'YYYY-WW') AS year_week,
        a.channel_name,
        a.brand_name,
        COUNT(DISTINCT a.application_id)            AS applications,
        SUM(a.is_approved)                          AS approvals,
        COUNT(DISTINCT c.loan_id)                   AS disbursements,
        SUM(c.principal_amount_zar)                 AS total_disbursed_zar,
        SUM(rs.total_paid_zar)                      AS total_collected_zar,
        AVG(a.credit_score::FLOAT)                  AS avg_credit_score,
        AVG(a.dsr_ratio)                            AS avg_dsr,
        SUM(CASE WHEN c.loan_status IN ('ARREARS','WRITTEN_OFF') THEN 1 ELSE 0 END)
                                                    AS defaults,
        -- Channel quality components
        AVG(1.0 - COALESCE(c.is_default, 0))        AS pct_non_default,
        AVG(CASE WHEN a.derived_risk_band IN ('LOW','MEDIUM') THEN 1.0 ELSE 0.0 END)
                                                    AS pct_low_risk,
        AVG(COALESCE(rs.collection_rate, 0))        AS avg_collection_rate,
        AVG(COALESCE(a.credit_score, 580)::FLOAT / 850) AS avg_norm_score
    FROM apps a
    LEFT JOIN contracts c     ON a.application_id  = c.application_id
    LEFT JOIN repay_stats rs  ON c.loan_id          = rs.loan_id
    GROUP BY 1, 2, 3
),
with_quality AS (
    SELECT
        *,
        (pct_non_default * 0.35 + pct_low_risk * 0.25
         + avg_collection_rate * 0.20 + avg_norm_score * 0.20) AS channel_quality_score
    FROM base
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['year_week', 'channel_name', 'brand_name']) }} AS roi_sk,
    year_week,
    channel_name,
    brand_name,
    applications,
    approvals,
    disbursements,
    total_disbursed_zar,
    total_collected_zar,
    avg_credit_score,
    avg_dsr,
    defaults,
    channel_quality_score,
    CASE WHEN applications > 0 THEN approvals::FLOAT / applications ELSE NULL END  AS approval_rate,
    CASE WHEN disbursements > 0 THEN defaults::FLOAT / disbursements ELSE NULL END AS default_rate,
    avg_collection_rate                                                              AS collection_rate,
    CURRENT_TIMESTAMP                                                               AS _mart_built_ts
FROM with_quality

{% if is_incremental() %}
WHERE year_week > (SELECT MAX(year_week) FROM {{ this }})
{% endif %}
