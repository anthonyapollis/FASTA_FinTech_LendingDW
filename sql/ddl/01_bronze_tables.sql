-- =============================================================================
-- FASTA FinTech Lending DW — Bronze Layer DDL
-- Operational source tables (raw ingestion, no transformations)
-- Target: SQL Server / Synapse Dedicated Pool / Fabric Warehouse
-- Author: Anthony Apollis | 2026-06-17
-- =============================================================================

-- ----------------------------------------------------------------------------
-- SCHEMA
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze;

-- ----------------------------------------------------------------------------
-- CUSTOMERS  (PII: ID numbers hashed, mobile/bank masked)
-- ----------------------------------------------------------------------------
CREATE TABLE bronze.customers (
    customer_id             VARCHAR(36)      NOT NULL,
    id_number_hash          VARCHAR(64)      NOT NULL,   -- SHA-256 of SA ID — never store raw
    mobile_masked           VARCHAR(15),                 -- e.g. +27 ** *** **89
    first_name              VARCHAR(80),
    last_name               VARCHAR(80),
    email_address           VARCHAR(120),
    date_of_birth           VARCHAR(30),                 -- intentionally VARCHAR — dirty dates
    gender                  VARCHAR(20),
    employment_status       VARCHAR(30),
    employer_name           VARCHAR(120),
    net_income_zar          DECIMAL(14,2),
    province                VARCHAR(40),
    city                    VARCHAR(60),
    registration_date       VARCHAR(30),
    kyc_status              VARCHAR(20),
    _ingest_timestamp       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    _source_file            VARCHAR(200),
    CONSTRAINT pk_brnz_customers PRIMARY KEY (customer_id)
);

-- ----------------------------------------------------------------------------
-- CAMPAIGNS
-- ----------------------------------------------------------------------------
CREATE TABLE bronze.campaigns (
    campaign_id             VARCHAR(36)      NOT NULL,
    campaign_name           VARCHAR(120),
    channel_name            VARCHAR(60),
    campaign_type           VARCHAR(40),
    start_date              VARCHAR(30),
    end_date                VARCHAR(30),
    budget_zar              DECIMAL(14,2),
    spend_zar               DECIMAL(14,2),
    impressions             INT,
    clicks                  INT,
    brand_name              VARCHAR(60),
    _ingest_timestamp       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    _source_file            VARCHAR(200),
    CONSTRAINT pk_brnz_campaigns PRIMARY KEY (campaign_id)
);

-- ----------------------------------------------------------------------------
-- WEB SESSIONS
-- ----------------------------------------------------------------------------
CREATE TABLE bronze.web_sessions (
    session_id              VARCHAR(36)      NOT NULL,
    customer_id             VARCHAR(36),
    campaign_id             VARCHAR(36),
    session_date            VARCHAR(30),
    session_start_ts        VARCHAR(30),
    session_end_ts          VARCHAR(30),
    channel_name            VARCHAR(60),
    device_type             VARCHAR(30),
    landing_page            VARCHAR(200),
    exit_page               VARCHAR(200),
    page_views              INT,
    is_bot                  TINYINT,
    brand_name              VARCHAR(60),
    _ingest_timestamp       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_brnz_sessions PRIMARY KEY (session_id)
);

-- ----------------------------------------------------------------------------
-- WEB EVENTS
-- ----------------------------------------------------------------------------
CREATE TABLE bronze.web_events (
    event_id                VARCHAR(36)      NOT NULL,
    session_id              VARCHAR(36),
    event_type              VARCHAR(60),
    event_timestamp         VARCHAR(30),
    page_url                VARCHAR(300),
    event_value_zar         DECIMAL(14,2),
    _ingest_timestamp       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_brnz_events PRIMARY KEY (event_id)
);

-- ----------------------------------------------------------------------------
-- LOAN APPLICATIONS
-- ----------------------------------------------------------------------------
CREATE TABLE bronze.loan_applications (
    application_id          VARCHAR(36)      NOT NULL,
    customer_id             VARCHAR(36),
    application_date        VARCHAR(30),
    channel_name            VARCHAR(60),
    loan_amount_requested   DECIMAL(14,2),
    loan_term_months        INT,
    purpose                 VARCHAR(80),
    application_status      VARCHAR(30),
    brand_name              VARCHAR(60),
    campaign_id             VARCHAR(36),
    _ingest_timestamp       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_brnz_apps PRIMARY KEY (application_id)
);

-- ----------------------------------------------------------------------------
-- CREDIT CHECKS
-- ----------------------------------------------------------------------------
CREATE TABLE bronze.credit_checks (
    credit_check_id         VARCHAR(36)      NOT NULL,
    application_id          VARCHAR(36),
    check_date              VARCHAR(30),
    bureau_name             VARCHAR(60),
    credit_score            INT,
    risk_band               VARCHAR(20),
    enquiry_result          VARCHAR(20),
    adverse_listings_count  INT,
    open_accounts_count     INT,
    total_outstanding_debt  DECIMAL(14,2),
    monthly_obligations     DECIMAL(14,2),
    judgements_count        INT,
    _ingest_timestamp       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_brnz_checks PRIMARY KEY (credit_check_id)
);

-- ----------------------------------------------------------------------------
-- AFFORDABILITY ASSESSMENTS
-- ----------------------------------------------------------------------------
CREATE TABLE bronze.affordability_assessments (
    assessment_id           VARCHAR(36)      NOT NULL,
    application_id          VARCHAR(36),
    assessment_date         VARCHAR(30),
    net_income_zar          DECIMAL(14,2),
    declared_expenses_zar   DECIMAL(14,2),
    nett_disposable_income  DECIMAL(14,2),
    dsr_ratio               DECIMAL(6,4),
    assessment_result       VARCHAR(20),
    _ingest_timestamp       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_brnz_afford PRIMARY KEY (assessment_id)
);

-- ----------------------------------------------------------------------------
-- LOAN CONTRACTS
-- ----------------------------------------------------------------------------
CREATE TABLE bronze.loan_contracts (
    loan_id                 VARCHAR(36)      NOT NULL,
    application_id          VARCHAR(36),
    contract_number         VARCHAR(30),
    disbursal_date          VARCHAR(30),
    principal_amount_zar    DECIMAL(14,2),
    interest_rate_annual    DECIMAL(6,4),
    term_months             INT,
    monthly_instalment      DECIMAL(14,2),
    total_repayable_zar     DECIMAL(14,2),
    loan_status             VARCHAR(30),
    bank_account_masked     VARCHAR(20),     -- last 4 digits only
    _ingest_timestamp       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_brnz_contracts PRIMARY KEY (loan_id)
);

-- ----------------------------------------------------------------------------
-- REPAYMENT SCHEDULE
-- ----------------------------------------------------------------------------
CREATE TABLE bronze.repayment_schedule (
    schedule_id             VARCHAR(36)      NOT NULL,
    loan_id                 VARCHAR(36),
    due_date                VARCHAR(30),
    instalment_number       INT,
    instalment_amount_zar   DECIMAL(14,2),
    principal_component     DECIMAL(14,2),
    interest_component      DECIMAL(14,2),
    schedule_status         VARCHAR(20),
    _ingest_timestamp       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_brnz_schedule PRIMARY KEY (schedule_id)
);

-- ----------------------------------------------------------------------------
-- PAYMENTS
-- ----------------------------------------------------------------------------
CREATE TABLE bronze.payments (
    payment_id              VARCHAR(36)      NOT NULL,
    loan_id                 VARCHAR(36),
    payment_date            VARCHAR(30),
    payment_amount_zar      DECIMAL(14,2),
    payment_method          VARCHAR(30),
    payment_status          VARCHAR(20),
    reference_number        VARCHAR(40),
    _ingest_timestamp       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_brnz_payments PRIMARY KEY (payment_id)
);

-- ----------------------------------------------------------------------------
-- FEE CHARGES
-- ----------------------------------------------------------------------------
CREATE TABLE bronze.fee_charges (
    fee_id                  VARCHAR(36)      NOT NULL,
    loan_id                 VARCHAR(36),
    fee_date                VARCHAR(30),
    fee_type                VARCHAR(40),
    fee_amount_zar          DECIMAL(14,2),
    fee_status              VARCHAR(20),
    _ingest_timestamp       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_brnz_fees PRIMARY KEY (fee_id)
);

-- ----------------------------------------------------------------------------
-- COLLECTION ACTIONS
-- ----------------------------------------------------------------------------
CREATE TABLE bronze.collection_actions (
    action_id               VARCHAR(36)      NOT NULL,
    loan_id                 VARCHAR(36),
    action_date             VARCHAR(30),
    action_type             VARCHAR(40),
    outcome                 VARCHAR(80),
    agent_id                VARCHAR(36),
    _ingest_timestamp       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_brnz_collections PRIMARY KEY (action_id)
);

-- ----------------------------------------------------------------------------
-- SUPPORT TICKETS
-- ----------------------------------------------------------------------------
CREATE TABLE bronze.support_tickets (
    ticket_id               VARCHAR(36)      NOT NULL,
    customer_id             VARCHAR(36),
    loan_id                 VARCHAR(36),
    created_date            VARCHAR(30),
    category                VARCHAR(60),
    priority                VARCHAR(20),
    status                  VARCHAR(20),
    resolution_date         VARCHAR(30),
    satisfaction_score      INT,
    _ingest_timestamp       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_brnz_tickets PRIMARY KEY (ticket_id)
);

-- ----------------------------------------------------------------------------
-- REVIEWS
-- ----------------------------------------------------------------------------
CREATE TABLE bronze.reviews (
    review_id               VARCHAR(36)      NOT NULL,
    customer_id             VARCHAR(36),
    review_date             VARCHAR(30),
    platform                VARCHAR(40),
    rating                  INT,
    sentiment               VARCHAR(20),
    review_text             NVARCHAR(2000),
    _ingest_timestamp       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_brnz_reviews PRIMARY KEY (review_id)
);

-- ----------------------------------------------------------------------------
-- PARTNER REFERRALS
-- ----------------------------------------------------------------------------
CREATE TABLE bronze.partner_referrals (
    referral_id             VARCHAR(36)      NOT NULL,
    customer_id             VARCHAR(36),
    partner_name            VARCHAR(80),
    referral_date           VARCHAR(30),
    referral_code           VARCHAR(20),
    commission_zar          DECIMAL(14,2),
    _ingest_timestamp       DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_brnz_referrals PRIMARY KEY (referral_id)
);

-- ----------------------------------------------------------------------------
-- WATERMARK TABLE (pipeline control)
-- ----------------------------------------------------------------------------
CREATE TABLE bronze._pipeline_watermark (
    table_name              VARCHAR(80)      NOT NULL,
    last_load_ts            DATETIME2        NOT NULL DEFAULT '1900-01-01',
    rows_loaded             BIGINT           DEFAULT 0,
    load_status             VARCHAR(20)      DEFAULT 'PENDING',
    CONSTRAINT pk_watermark PRIMARY KEY (table_name)
);

INSERT INTO bronze._pipeline_watermark (table_name) VALUES
('customers'),('campaigns'),('web_sessions'),('web_events'),
('loan_applications'),('credit_checks'),('affordability_assessments'),
('loan_contracts'),('repayment_schedule'),('payments'),('fee_charges'),
('collection_actions'),('support_tickets'),('reviews'),('partner_referrals');
