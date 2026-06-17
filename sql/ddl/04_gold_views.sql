-- =============================================================================
-- FASTA FinTech Lending DW — Gold Layer Views
-- Business-facing views used by Power BI reports
-- Author: Anthony Apollis | 2026-06-17
-- =============================================================================

-- ----------------------------------------------------------------------------
-- vw_loan_funnel — full application-to-repayment journey per channel
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW gold.vw_loan_funnel AS
SELECT
    dd.year_num,
    dd.month_name,
    dd.quarter_name,
    dc.channel_name,
    dc.channel_category,
    db.brand_name,
    dr.risk_band_name,
    COUNT(DISTINCT fa.application_id)     AS applications,
    SUM(fa.is_approved)                   AS approvals,
    SUM(fa.is_funded)                     AS funded_loans,
    SUM(fa.is_declined)                   AS declined,
    AVG(CAST(fa.credit_score AS FLOAT))   AS avg_credit_score,
    AVG(fa.dsr_ratio)                     AS avg_dsr,
    SUM(fd.principal_amount_zar)          AS total_disbursed_zar,
    SUM(fd.is_default)                    AS defaults,
    CASE WHEN COUNT(fa.application_id) > 0
         THEN CAST(SUM(fa.is_approved) AS FLOAT) / COUNT(fa.application_id)
         ELSE NULL END                    AS approval_rate,
    CASE WHEN SUM(fa.is_approved) > 0
         THEN CAST(SUM(fa.is_funded) AS FLOAT) / NULLIF(SUM(fa.is_approved),0)
         ELSE NULL END                    AS funding_rate
FROM gold.fact_loan_application fa
JOIN gold.dim_date      dd ON fa.date_key    = dd.date_key
JOIN gold.dim_channel   dc ON fa.channel_sk  = dc.channel_sk
JOIN gold.dim_brand     db ON fa.brand_sk    = db.brand_sk
JOIN gold.dim_risk_band dr ON fa.risk_band_sk= dr.risk_band_sk
LEFT JOIN gold.fact_disbursement fd ON fa.application_sk = fd.application_sk
GROUP BY
    dd.year_num, dd.month_name, dd.quarter_name,
    dc.channel_name, dc.channel_category,
    db.brand_name, dr.risk_band_name;

-- ----------------------------------------------------------------------------
-- vw_repayment_health — collection performance per loan
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW gold.vw_repayment_health AS
SELECT
    fd.loan_id,
    dc.channel_name,
    db.brand_name,
    dr.risk_band_name,
    fd.principal_amount_zar,
    fd.total_repayable_zar,
    fd.loan_status,
    fd.is_default,
    SUM(fr.payment_amount_zar)            AS total_collected_zar,
    COUNT(fr.repayment_sk)                AS payment_count,
    SUM(CASE WHEN fr.is_successful = 1 THEN 1 ELSE 0 END) AS successful_payments,
    SUM(CASE WHEN fr.is_reversal   = 1 THEN 1 ELSE 0 END) AS reversals,
    AVG(fr.days_overdue)                  AS avg_days_overdue,
    CASE WHEN fd.total_repayable_zar > 0
         THEN SUM(fr.payment_amount_zar) / fd.total_repayable_zar
         ELSE NULL END                    AS collection_rate
FROM gold.fact_disbursement fd
JOIN gold.dim_channel   dc ON fd.channel_sk   = dc.channel_sk
JOIN gold.dim_brand     db ON fd.brand_sk     = db.brand_sk
JOIN gold.dim_risk_band dr ON fd.risk_band_sk = dr.risk_band_sk
LEFT JOIN gold.fact_repayment fr ON fd.disbursement_sk = fr.disbursement_sk
GROUP BY
    fd.loan_id, dc.channel_name, db.brand_name,
    dr.risk_band_name, fd.principal_amount_zar,
    fd.total_repayable_zar, fd.loan_status, fd.is_default;

-- ----------------------------------------------------------------------------
-- vw_channel_quality — composite quality score by channel (for Power BI)
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW gold.vw_channel_quality AS
SELECT
    dc.channel_name,
    dc.channel_category,
    COUNT(DISTINCT fd.loan_id)            AS n_loans,
    AVG(CAST(fa.credit_score AS FLOAT))   AS avg_credit_score,
    AVG(fd.principal_amount_zar)          AS avg_loan_amount,
    AVG(CAST(fd.is_default AS FLOAT))     AS default_rate,
    AVG(mr.collection_rate)               AS collection_rate,
    MAX(mr.channel_quality_score)         AS quality_score,
    SUM(mr.total_disbursed_zar)           AS total_disbursed_zar,
    SUM(mr.campaign_spend_zar)            AS total_spend_zar,
    CASE WHEN SUM(mr.campaign_spend_zar) > 0
         THEN SUM(mr.total_disbursed_zar) / SUM(mr.campaign_spend_zar)
         ELSE NULL END                    AS roi_multiple
FROM gold.fact_disbursement fd
JOIN gold.dim_channel        dc ON fd.channel_sk   = dc.channel_sk
JOIN gold.fact_loan_application fa ON fd.application_sk = fa.application_sk
LEFT JOIN gold.mart_channel_roi mr ON fd.channel_sk = mr.channel_sk
GROUP BY dc.channel_name, dc.channel_category;

-- ----------------------------------------------------------------------------
-- vw_monthly_kpis — executive-level monthly scorecard
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW gold.vw_monthly_kpis AS
SELECT
    dd.year_num,
    dd.month_num,
    dd.month_name,
    dd.quarter_name,
    -- Volume
    COUNT(DISTINCT fa.application_id)     AS total_applications,
    SUM(fa.is_approved)                   AS approvals,
    SUM(fa.is_funded)                     AS disbursements,
    SUM(fd.principal_amount_zar)          AS total_disbursed_zar,
    -- Quality
    AVG(CAST(fa.credit_score AS FLOAT))   AS avg_credit_score,
    AVG(fa.dsr_ratio)                     AS avg_dsr,
    SUM(fd.is_default)                    AS defaults,
    -- Rates
    CAST(SUM(fa.is_approved) AS FLOAT)
      / NULLIF(COUNT(fa.application_id),0) AS approval_rate,
    CAST(SUM(fd.is_default) AS FLOAT)
      / NULLIF(SUM(fa.is_funded),0)        AS default_rate
FROM gold.fact_loan_application fa
JOIN gold.dim_date dd           ON fa.date_key     = dd.date_key
LEFT JOIN gold.fact_disbursement fd ON fa.application_sk = fd.application_sk
GROUP BY dd.year_num, dd.month_num, dd.month_name, dd.quarter_name;

-- ----------------------------------------------------------------------------
-- vw_web_to_loan — digital funnel conversion rates
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW gold.vw_web_to_loan AS
SELECT
    dd.year_num,
    dd.month_name,
    dc.channel_name,
    db.brand_name,
    SUM(ff.session_count)                 AS sessions,
    SUM(ff.application_submissions)       AS app_submissions,
    SUM(ff.quote_completions)             AS quote_completions,
    CAST(SUM(ff.application_submissions) AS FLOAT)
      / NULLIF(SUM(ff.session_count),0)   AS session_to_app_rate,
    CAST(SUM(ff.quote_completions) AS FLOAT)
      / NULLIF(SUM(ff.session_count),0)   AS session_to_quote_rate
FROM gold.fact_web_funnel ff
JOIN gold.dim_date    dd ON ff.date_key   = dd.date_key
JOIN gold.dim_channel dc ON ff.channel_sk = dc.channel_sk
JOIN gold.dim_brand   db ON ff.brand_sk   = db.brand_sk
GROUP BY dd.year_num, dd.month_name, dc.channel_name, db.brand_name;
