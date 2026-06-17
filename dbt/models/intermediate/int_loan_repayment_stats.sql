-- dbt intermediate model: int_loan_repayment_stats
-- Aggregates payment history per loan for collection analytics
-- Materialisation: table

{{ config(materialized='table', schema='intermediate') }}

WITH payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
),
schedule AS (
    SELECT
        loan_id,
        SUM(instalment_amount_zar)  AS total_scheduled_zar,
        COUNT(*)                    AS total_instalments,
        SUM(CASE WHEN schedule_status = 'OVERDUE' THEN 1 ELSE 0 END) AS overdue_instalments,
        MIN(due_date)               AS first_due_date,
        MAX(due_date)               AS last_due_date
    FROM {{ source('silver', 'repayment_schedule') }}
    GROUP BY loan_id
),
pay_agg AS (
    SELECT
        loan_id,
        COUNT(*)                                    AS total_payment_attempts,
        SUM(CASE WHEN is_successful = 1 THEN 1 ELSE 0 END)  AS successful_payments,
        SUM(CASE WHEN is_reversal   = 1 THEN 1 ELSE 0 END)  AS reversals,
        SUM(payment_amount_zar)                     AS total_paid_zar,
        MAX(payment_date)                           AS last_payment_date,
        MIN(payment_date)                           AS first_payment_date,
        AVG(payment_amount_zar)                     AS avg_payment_zar
    FROM payments
    GROUP BY loan_id
)

SELECT
    COALESCE(s.loan_id, p.loan_id)             AS loan_id,
    s.total_scheduled_zar,
    s.total_instalments,
    s.overdue_instalments,
    s.first_due_date,
    s.last_due_date,
    p.total_payment_attempts,
    p.successful_payments,
    p.reversals,
    p.total_paid_zar,
    p.last_payment_date,
    p.first_payment_date,
    CASE WHEN s.total_scheduled_zar > 0
         THEN p.total_paid_zar / s.total_scheduled_zar
         ELSE NULL END                              AS collection_rate,
    CASE WHEN p.total_payment_attempts > 0
         THEN p.successful_payments::FLOAT / p.total_payment_attempts
         ELSE NULL END                              AS payment_success_rate,
    -- Churn signal: days since last successful payment
    DATEDIFF('day', p.last_payment_date, CURRENT_DATE) AS days_since_last_payment
FROM schedule s
FULL OUTER JOIN pay_agg p ON s.loan_id = p.loan_id
