-- =============================================================================
-- FASTA FinTech Lending DW — Silver Layer DDL
-- Cleansed, standardised, DQ-flagged tables (Delta / Fabric Lakehouse)
-- All dates: DATE type | Amounts: DECIMAL(14,2), negative → NULL
-- Author: Anthony Apollis | 2026-06-17
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS silver;

-- ----------------------------------------------------------------------------
-- CUSTOMERS (cleansed)
-- ----------------------------------------------------------------------------
CREATE TABLE silver.customers (
    customer_id             VARCHAR(36)      NOT NULL,
    id_number_hash          VARCHAR(64)      NOT NULL,
    mobile_masked           VARCHAR(15),
    first_name              VARCHAR(80),
    last_name               VARCHAR(80),
    full_name               VARCHAR(160),                -- computed: initCap(first + last)
    email_address           VARCHAR(120),
    date_of_birth           DATE,                        -- normalised from 6 dirty formats
    age_years               INT,
    age_band                VARCHAR(20),                 -- <25 | 25-34 | 35-44 | 45-54 | 55+
    gender                  VARCHAR(10),                 -- M | F | OTHER | UNKNOWN
    employment_status       VARCHAR(20),
    employer_name           VARCHAR(120),
    net_income_zar          DECIMAL(14,2),               -- clipped: < 0 → NULL
    income_band             VARCHAR(20),                 -- <8K | 8K-15K | 15K-25K | 25K-40K | 40K+
    province                VARCHAR(40),
    city                    VARCHAR(60),
    registration_date       DATE,
    kyc_status              VARCHAR(20),
    -- DQ flags
    dq_invalid_dob          TINYINT  DEFAULT 0,
    dq_invalid_income       TINYINT  DEFAULT 0,
    dq_missing_email        TINYINT  DEFAULT 0,
    dq_any_flag             TINYINT  DEFAULT 0,          -- 1 if any dq_ = 1
    _silver_load_ts         DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_slvr_customers PRIMARY KEY (customer_id)
);

-- ----------------------------------------------------------------------------
-- CAMPAIGNS (cleansed)
-- ----------------------------------------------------------------------------
CREATE TABLE silver.campaigns (
    campaign_id             VARCHAR(36)      NOT NULL,
    campaign_name           VARCHAR(120),
    channel_name            VARCHAR(60),
    campaign_type           VARCHAR(40),
    start_date              DATE,
    end_date                DATE,
    budget_zar              DECIMAL(14,2),
    spend_zar               DECIMAL(14,2),
    impressions             INT,
    clicks                  INT,
    ctr                     DECIMAL(8,6),               -- clicks / impressions
    spend_ratio             DECIMAL(8,4),               -- spend / budget
    brand_name              VARCHAR(60),
    dq_invalid_dates        TINYINT  DEFAULT 0,
    dq_negative_spend       TINYINT  DEFAULT 0,
    dq_any_flag             TINYINT  DEFAULT 0,
    _silver_load_ts         DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_slvr_campaigns PRIMARY KEY (campaign_id)
);

-- ----------------------------------------------------------------------------
-- WEB SESSIONS (cleansed, bots removed)
-- ----------------------------------------------------------------------------
CREATE TABLE silver.web_sessions (
    session_id              VARCHAR(36)      NOT NULL,
    customer_id             VARCHAR(36),
    campaign_id             VARCHAR(36),
    session_date            DATE,
    session_start_ts        DATETIME2,
    session_end_ts          DATETIME2,
    session_duration_sec    INT,
    channel_name            VARCHAR(60),
    device_type             VARCHAR(30),
    landing_page            VARCHAR(200),
    exit_page               VARCHAR(200),
    page_views              INT,
    brand_name              VARCHAR(60),
    dq_orphan_customer      TINYINT  DEFAULT 0,
    dq_any_flag             TINYINT  DEFAULT 0,
    _silver_load_ts         DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_slvr_sessions PRIMARY KEY (session_id)
    -- Note: is_bot = 1 rows EXCLUDED entirely (→ quarantine)
);

-- ----------------------------------------------------------------------------
-- WEB EVENTS (cleansed)
-- ----------------------------------------------------------------------------
CREATE TABLE silver.web_events (
    event_id                VARCHAR(36)      NOT NULL,
    session_id              VARCHAR(36),
    event_type              VARCHAR(60),
    event_timestamp         DATETIME2,
    event_date              DATE,
    page_url                VARCHAR(300),
    event_value_zar         DECIMAL(14,2),
    dq_orphan_session       TINYINT  DEFAULT 0,
    dq_negative_value       TINYINT  DEFAULT 0,
    dq_any_flag             TINYINT  DEFAULT 0,
    _silver_load_ts         DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_slvr_events PRIMARY KEY (event_id)
);

-- ----------------------------------------------------------------------------
-- LOAN APPLICATIONS (cleansed, status canonicalised)
-- ----------------------------------------------------------------------------
CREATE TABLE silver.loan_applications (
    application_id          VARCHAR(36)      NOT NULL,
    customer_id             VARCHAR(36),
    application_date        DATE,
    channel_name            VARCHAR(60),
    loan_amount_requested   DECIMAL(14,2),
    loan_term_months        INT,
    purpose                 VARCHAR(80),
    application_status      VARCHAR(20),    -- APPROVED | DECLINED | PENDING | CANCELLED | REFERRED | EXPIRED
    brand_name              VARCHAR(60),
    campaign_id             VARCHAR(36),
    dq_invalid_amount       TINYINT  DEFAULT 0,
    dq_orphan_customer      TINYINT  DEFAULT 0,
    dq_any_flag             TINYINT  DEFAULT 0,
    _silver_load_ts         DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_slvr_apps PRIMARY KEY (application_id)
);

-- ----------------------------------------------------------------------------
-- CREDIT CHECKS (cleansed, score validated 300–850)
-- ----------------------------------------------------------------------------
CREATE TABLE silver.credit_checks (
    credit_check_id         VARCHAR(36)      NOT NULL,
    application_id          VARCHAR(36),
    check_date              DATE,
    bureau_name             VARCHAR(60),
    credit_score            INT,            -- clamped 300–850, NULL if out of range
    risk_band               VARCHAR(20),    -- LOW | MEDIUM | HIGH | VERY_HIGH
    enquiry_result          VARCHAR(20),    -- PASS | FAIL | REFER
    adverse_listings_count  INT,
    open_accounts_count     INT,
    total_outstanding_debt  DECIMAL(14,2),
    monthly_obligations     DECIMAL(14,2),
    judgements_count        INT,
    dq_invalid_score        TINYINT  DEFAULT 0,
    dq_orphan_application   TINYINT  DEFAULT 0,
    dq_any_flag             TINYINT  DEFAULT 0,
    _silver_load_ts         DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_slvr_checks PRIMARY KEY (credit_check_id)
);

-- ----------------------------------------------------------------------------
-- AFFORDABILITY ASSESSMENTS (cleansed, derived DSR)
-- ----------------------------------------------------------------------------
CREATE TABLE silver.affordability_assessments (
    assessment_id           VARCHAR(36)      NOT NULL,
    application_id          VARCHAR(36),
    assessment_date         DATE,
    net_income_zar          DECIMAL(14,2),
    declared_expenses_zar   DECIMAL(14,2),
    nett_disposable_income  DECIMAL(14,2),
    dsr_ratio               DECIMAL(6,4),
    expense_ratio           DECIMAL(6,4),   -- declared_expenses / net_income
    debt_ratio              DECIMAL(6,4),   -- outstanding_debt / (net_income*12)
    assessment_result       VARCHAR(20),    -- PASS | FAIL | BORDERLINE
    dq_invalid_income       TINYINT  DEFAULT 0,
    dq_dsr_exceeds_limit    TINYINT  DEFAULT 0,   -- > 0.45 NCA limit
    dq_any_flag             TINYINT  DEFAULT 0,
    _silver_load_ts         DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_slvr_afford PRIMARY KEY (assessment_id)
);

-- ----------------------------------------------------------------------------
-- LOAN CONTRACTS (cleansed)
-- ----------------------------------------------------------------------------
CREATE TABLE silver.loan_contracts (
    loan_id                 VARCHAR(36)      NOT NULL,
    application_id          VARCHAR(36),
    contract_number         VARCHAR(30),
    disbursal_date          DATE,
    principal_amount_zar    DECIMAL(14,2),
    interest_rate_annual    DECIMAL(6,4),
    term_months             INT,
    monthly_instalment      DECIMAL(14,2),
    total_repayable_zar     DECIMAL(14,2),
    loan_status             VARCHAR(20),    -- ACTIVE | SETTLED | ARREARS | WRITTEN_OFF | CANCELLED
    bank_account_masked     VARCHAR(20),
    dq_invalid_amount       TINYINT  DEFAULT 0,
    dq_orphan_application   TINYINT  DEFAULT 0,
    dq_any_flag             TINYINT  DEFAULT 0,
    _silver_load_ts         DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_slvr_contracts PRIMARY KEY (loan_id)
);

-- ----------------------------------------------------------------------------
-- REPAYMENT SCHEDULE (cleansed)
-- ----------------------------------------------------------------------------
CREATE TABLE silver.repayment_schedule (
    schedule_id             VARCHAR(36)      NOT NULL,
    loan_id                 VARCHAR(36),
    due_date                DATE,
    instalment_number       INT,
    instalment_amount_zar   DECIMAL(14,2),
    principal_component     DECIMAL(14,2),
    interest_component      DECIMAL(14,2),
    schedule_status         VARCHAR(20),    -- PENDING | PAID | OVERDUE | WAIVED
    days_overdue            INT,
    dq_orphan_loan          TINYINT  DEFAULT 0,
    dq_any_flag             TINYINT  DEFAULT 0,
    _silver_load_ts         DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_slvr_schedule PRIMARY KEY (schedule_id)
);

-- ----------------------------------------------------------------------------
-- PAYMENTS (cleansed, future dates nulled)
-- ----------------------------------------------------------------------------
CREATE TABLE silver.payments (
    payment_id              VARCHAR(36)      NOT NULL,
    loan_id                 VARCHAR(36),
    payment_date            DATE,           -- NULL if was > current date in Bronze
    payment_amount_zar      DECIMAL(14,2),  -- NULL if was negative
    payment_method          VARCHAR(30),
    payment_status          VARCHAR(20),    -- SUCCESS | FAILED | REVERSED | PENDING
    reference_number        VARCHAR(40),
    dq_future_date          TINYINT  DEFAULT 0,
    dq_negative_amount      TINYINT  DEFAULT 0,
    dq_orphan_loan          TINYINT  DEFAULT 0,
    dq_any_flag             TINYINT  DEFAULT 0,
    _silver_load_ts         DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_slvr_payments PRIMARY KEY (payment_id)
);

-- ----------------------------------------------------------------------------
-- FEE CHARGES (cleansed)
-- ----------------------------------------------------------------------------
CREATE TABLE silver.fee_charges (
    fee_id                  VARCHAR(36)      NOT NULL,
    loan_id                 VARCHAR(36),
    fee_date                DATE,
    fee_type                VARCHAR(40),
    fee_amount_zar          DECIMAL(14,2),
    fee_status              VARCHAR(20),
    dq_negative_amount      TINYINT  DEFAULT 0,
    dq_any_flag             TINYINT  DEFAULT 0,
    _silver_load_ts         DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_slvr_fees PRIMARY KEY (fee_id)
);

-- ----------------------------------------------------------------------------
-- COLLECTION ACTIONS (cleansed)
-- ----------------------------------------------------------------------------
CREATE TABLE silver.collection_actions (
    action_id               VARCHAR(36)      NOT NULL,
    loan_id                 VARCHAR(36),
    action_date             DATE,
    action_type             VARCHAR(40),
    outcome                 VARCHAR(80),
    agent_id                VARCHAR(36),
    is_promise_to_pay       TINYINT  DEFAULT 0,  -- 1 if outcome contains PAYMENT/PAY/RECEIVED
    dq_orphan_loan          TINYINT  DEFAULT 0,
    dq_any_flag             TINYINT  DEFAULT 0,
    _silver_load_ts         DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_slvr_collections PRIMARY KEY (action_id)
);

-- ----------------------------------------------------------------------------
-- DQ QUARANTINE (rows failing Silver thresholds)
-- ----------------------------------------------------------------------------
CREATE TABLE silver._quarantine (
    quarantine_id           BIGINT           NOT NULL IDENTITY(1,1),
    source_table            VARCHAR(80)      NOT NULL,
    source_pk               VARCHAR(36),
    dq_rule_violated        VARCHAR(80),
    raw_value               NVARCHAR(500),
    quarantine_ts           DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_quarantine PRIMARY KEY (quarantine_id)
);

-- Silver load log
CREATE TABLE silver._load_log (
    log_id                  BIGINT           NOT NULL IDENTITY(1,1),
    table_name              VARCHAR(80),
    load_date               DATE,
    rows_loaded             BIGINT,
    rows_quarantined        BIGINT,
    load_duration_sec       INT,
    load_status             VARCHAR(20),
    CONSTRAINT pk_slvr_log PRIMARY KEY (log_id)
);
