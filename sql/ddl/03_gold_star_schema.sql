-- =============================================================================
-- FASTA FinTech Lending DW — Gold Layer DDL (Star Schema)
-- Dimensional model for Power BI DirectQuery / Fabric Warehouse
-- Author: Anthony Apollis | 2026-06-17
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- DIM_DATE  (calendar spine 2024-01-01 → 2027-12-31)
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_date (
    date_key                INT              NOT NULL,   -- YYYYMMDD
    full_date               DATE             NOT NULL,
    day_of_week             TINYINT,                    -- 1=Mon, 7=Sun
    day_name                VARCHAR(10),
    day_of_month            TINYINT,
    day_of_year             SMALLINT,
    week_of_year            TINYINT,
    iso_week                TINYINT,
    month_num               TINYINT,
    month_name              VARCHAR(10),
    month_short             CHAR(3),
    quarter_num             TINYINT,
    quarter_name            CHAR(2),                    -- Q1..Q4
    half_year               TINYINT,
    year_num                SMALLINT,
    fiscal_year             SMALLINT,                   -- FASTA fiscal: Mar–Feb
    fiscal_quarter          TINYINT,
    is_weekend              BIT              DEFAULT 0,
    is_public_holiday_za    BIT              DEFAULT 0,
    holiday_name_za         VARCHAR(60),
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);

-- ----------------------------------------------------------------------------
-- DIM_CUSTOMER
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_customer (
    customer_sk             BIGINT           NOT NULL IDENTITY(1,1),
    customer_id             VARCHAR(36)      NOT NULL,   -- NK
    id_number_hash          VARCHAR(64)      NOT NULL,
    full_name               VARCHAR(160),
    age_band                VARCHAR(20),
    gender                  VARCHAR(10),
    employment_status       VARCHAR(20),
    income_band             VARCHAR(20),
    province                VARCHAR(40),
    city                    VARCHAR(60),
    kyc_status              VARCHAR(20),
    registration_date       DATE,
    -- SCD Type 2 columns
    effective_from          DATE             NOT NULL DEFAULT '2024-01-01',
    effective_to            DATE             NOT NULL DEFAULT '9999-12-31',
    is_current              BIT              DEFAULT 1,
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)
);
CREATE UNIQUE INDEX ux_dim_customer_nk ON gold.dim_customer (customer_id, effective_from);

-- ----------------------------------------------------------------------------
-- DIM_BRAND
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_brand (
    brand_sk                INT              NOT NULL IDENTITY(1,1),
    brand_name              VARCHAR(60)      NOT NULL,
    brand_segment           VARCHAR(40),                -- PRIME | NEAR-PRIME | SUB-PRIME
    CONSTRAINT pk_dim_brand PRIMARY KEY (brand_sk)
);

-- ----------------------------------------------------------------------------
-- DIM_CHANNEL
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_channel (
    channel_sk              INT              NOT NULL IDENTITY(1,1),
    channel_name            VARCHAR(60)      NOT NULL,
    channel_category        VARCHAR(30),                -- PAID | ORGANIC | DIRECT | REFERRAL
    is_digital              BIT              DEFAULT 1,
    CONSTRAINT pk_dim_channel PRIMARY KEY (channel_sk)
);

-- ----------------------------------------------------------------------------
-- DIM_CAMPAIGN
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_campaign (
    campaign_sk             INT              NOT NULL IDENTITY(1,1),
    campaign_id             VARCHAR(36)      NOT NULL,
    campaign_name           VARCHAR(120),
    campaign_type           VARCHAR(40),
    channel_sk              INT,
    brand_sk                INT,
    start_date              DATE,
    end_date                DATE,
    budget_zar              DECIMAL(14,2),
    CONSTRAINT pk_dim_campaign PRIMARY KEY (campaign_sk),
    CONSTRAINT fk_dcampaign_channel FOREIGN KEY (channel_sk) REFERENCES gold.dim_channel (channel_sk),
    CONSTRAINT fk_dcampaign_brand   FOREIGN KEY (brand_sk)   REFERENCES gold.dim_brand   (brand_sk)
);

-- ----------------------------------------------------------------------------
-- DIM_RISK_BAND
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_risk_band (
    risk_band_sk            TINYINT          NOT NULL,
    risk_band_name          VARCHAR(20)      NOT NULL,   -- LOW | MEDIUM | HIGH | VERY_HIGH | UNKNOWN
    risk_band_order         TINYINT,
    min_credit_score        INT,
    max_credit_score        INT,
    default_rate_expected   DECIMAL(6,4),
    CONSTRAINT pk_dim_risk_band PRIMARY KEY (risk_band_sk)
);
INSERT INTO gold.dim_risk_band VALUES
(1,'LOW',      1, 650, 850, 0.05),
(2,'MEDIUM',   2, 580, 649, 0.12),
(3,'HIGH',     3, 450, 579, 0.22),
(4,'VERY_HIGH',4, 300, 449, 0.38),
(5,'UNKNOWN',  5, NULL, NULL, NULL);

-- ----------------------------------------------------------------------------
-- DIM_PRODUCT
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_product (
    product_sk              INT              NOT NULL IDENTITY(1,1),
    product_name            VARCHAR(80)      NOT NULL,
    product_category        VARCHAR(40),                -- SHORT_TERM | MEDIUM_TERM | LONG_TERM
    min_amount_zar          DECIMAL(14,2),
    max_amount_zar          DECIMAL(14,2),
    min_term_months         INT,
    max_term_months         INT,
    CONSTRAINT pk_dim_product PRIMARY KEY (product_sk)
);
INSERT INTO gold.dim_product (product_name, product_category, min_amount_zar, max_amount_zar, min_term_months, max_term_months) VALUES
('Quick Cash',       'SHORT_TERM',  500,    5000,   1, 3),
('Personal Loan',    'MEDIUM_TERM', 5000,   50000,  6, 24),
('Home Improvement', 'LONG_TERM',  10000, 100000,  12, 60),
('Consolidation',    'MEDIUM_TERM', 5000,   80000,  12, 48);

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- FACT_WEB_FUNNEL  (one row per session)
-- ----------------------------------------------------------------------------
CREATE TABLE gold.fact_web_funnel (
    funnel_sk               BIGINT           NOT NULL IDENTITY(1,1),
    date_key                INT              NOT NULL,
    customer_sk             BIGINT,
    channel_sk              INT,
    campaign_sk             INT,
    brand_sk                INT,
    -- Measures
    session_count           INT              DEFAULT 1,
    page_views              INT,
    session_duration_sec    INT,
    event_count             INT,
    form_starts             INT,
    form_completions        INT,
    quote_starts            INT,
    quote_completions       INT,
    application_starts      INT,
    application_submissions INT,
    -- Funnel conversion rates (computed in Gold build)
    form_start_rate         DECIMAL(8,6),
    quote_completion_rate   DECIMAL(8,6),
    app_submission_rate     DECIMAL(8,6),
    CONSTRAINT pk_fact_funnel PRIMARY KEY (funnel_sk),
    CONSTRAINT fk_ff_date     FOREIGN KEY (date_key)     REFERENCES gold.dim_date     (date_key),
    CONSTRAINT fk_ff_customer FOREIGN KEY (customer_sk)  REFERENCES gold.dim_customer (customer_sk),
    CONSTRAINT fk_ff_channel  FOREIGN KEY (channel_sk)   REFERENCES gold.dim_channel  (channel_sk),
    CONSTRAINT fk_ff_campaign FOREIGN KEY (campaign_sk)  REFERENCES gold.dim_campaign (campaign_sk),
    CONSTRAINT fk_ff_brand    FOREIGN KEY (brand_sk)     REFERENCES gold.dim_brand    (brand_sk)
);

-- ----------------------------------------------------------------------------
-- FACT_LOAN_APPLICATION  (one row per application)
-- ----------------------------------------------------------------------------
CREATE TABLE gold.fact_loan_application (
    application_sk          BIGINT           NOT NULL IDENTITY(1,1),
    application_id          VARCHAR(36)      NOT NULL,
    date_key                INT              NOT NULL,   -- application_date
    customer_sk             BIGINT,
    channel_sk              INT,
    campaign_sk             INT,
    brand_sk                INT,
    risk_band_sk            TINYINT,
    -- Measures
    loan_amount_requested   DECIMAL(14,2),
    loan_term_months        INT,
    credit_score            INT,
    dsr_ratio               DECIMAL(6,4),
    net_income_zar          DECIMAL(14,2),
    -- Decision flags
    is_approved             BIT,
    is_declined             BIT,
    is_funded               BIT,
    time_to_decision_hrs    INT,
    -- Credit check results
    adverse_listings_count  INT,
    judgements_count        INT,
    total_outstanding_debt  DECIMAL(14,2),
    -- Affordability
    nett_disposable_income  DECIMAL(14,2),
    is_affordable           BIT,
    CONSTRAINT pk_fact_app PRIMARY KEY (application_sk),
    CONSTRAINT fk_fa_date      FOREIGN KEY (date_key)    REFERENCES gold.dim_date     (date_key),
    CONSTRAINT fk_fa_customer  FOREIGN KEY (customer_sk) REFERENCES gold.dim_customer (customer_sk),
    CONSTRAINT fk_fa_channel   FOREIGN KEY (channel_sk)  REFERENCES gold.dim_channel  (channel_sk),
    CONSTRAINT fk_fa_campaign  FOREIGN KEY (campaign_sk) REFERENCES gold.dim_campaign (campaign_sk),
    CONSTRAINT fk_fa_brand     FOREIGN KEY (brand_sk)    REFERENCES gold.dim_brand    (brand_sk),
    CONSTRAINT fk_fa_risk      FOREIGN KEY (risk_band_sk)REFERENCES gold.dim_risk_band(risk_band_sk)
);

-- ----------------------------------------------------------------------------
-- FACT_DISBURSEMENT  (one row per funded loan)
-- ----------------------------------------------------------------------------
CREATE TABLE gold.fact_disbursement (
    disbursement_sk         BIGINT           NOT NULL IDENTITY(1,1),
    loan_id                 VARCHAR(36)      NOT NULL,
    application_sk          BIGINT,
    date_key                INT              NOT NULL,   -- disbursal_date
    customer_sk             BIGINT,
    channel_sk              INT,
    brand_sk                INT,
    risk_band_sk            TINYINT,
    product_sk              INT,
    -- Measures
    principal_amount_zar    DECIMAL(14,2),
    total_repayable_zar     DECIMAL(14,2),
    monthly_instalment      DECIMAL(14,2),
    interest_rate_annual    DECIMAL(6,4),
    term_months             INT,
    loan_status             VARCHAR(20),
    is_default              BIT              DEFAULT 0,
    default_date_key        INT,
    CONSTRAINT pk_fact_disb PRIMARY KEY (disbursement_sk),
    CONSTRAINT fk_fd_date       FOREIGN KEY (date_key)    REFERENCES gold.dim_date     (date_key),
    CONSTRAINT fk_fd_customer   FOREIGN KEY (customer_sk) REFERENCES gold.dim_customer (customer_sk),
    CONSTRAINT fk_fd_channel    FOREIGN KEY (channel_sk)  REFERENCES gold.dim_channel  (channel_sk),
    CONSTRAINT fk_fd_brand      FOREIGN KEY (brand_sk)    REFERENCES gold.dim_brand    (brand_sk),
    CONSTRAINT fk_fd_risk       FOREIGN KEY (risk_band_sk)REFERENCES gold.dim_risk_band(risk_band_sk),
    CONSTRAINT fk_fd_product    FOREIGN KEY (product_sk)  REFERENCES gold.dim_product  (product_sk)
);

-- ----------------------------------------------------------------------------
-- FACT_REPAYMENT  (one row per payment transaction)
-- ----------------------------------------------------------------------------
CREATE TABLE gold.fact_repayment (
    repayment_sk            BIGINT           NOT NULL IDENTITY(1,1),
    payment_id              VARCHAR(36)      NOT NULL,
    date_key                INT              NOT NULL,   -- payment_date
    loan_id                 VARCHAR(36),
    disbursement_sk         BIGINT,
    customer_sk             BIGINT,
    channel_sk              INT,
    brand_sk                INT,
    risk_band_sk            TINYINT,
    -- Measures
    payment_amount_zar      DECIMAL(14,2),
    instalment_due_zar      DECIMAL(14,2),
    shortfall_zar           DECIMAL(14,2),              -- due - paid (>0 = underpaid)
    payment_method          VARCHAR(30),
    is_successful           BIT,
    is_reversal             BIT,
    days_overdue            INT,
    CONSTRAINT pk_fact_repay PRIMARY KEY (repayment_sk),
    CONSTRAINT fk_fr_date       FOREIGN KEY (date_key)      REFERENCES gold.dim_date     (date_key),
    CONSTRAINT fk_fr_customer   FOREIGN KEY (customer_sk)   REFERENCES gold.dim_customer (customer_sk),
    CONSTRAINT fk_fr_brand      FOREIGN KEY (brand_sk)      REFERENCES gold.dim_brand    (brand_sk),
    CONSTRAINT fk_fr_risk       FOREIGN KEY (risk_band_sk)  REFERENCES gold.dim_risk_band(risk_band_sk),
    CONSTRAINT fk_fr_disb       FOREIGN KEY (disbursement_sk) REFERENCES gold.fact_disbursement(disbursement_sk)
);

-- ============================================================================
-- GOLD MARTS (pre-aggregated for Power BI performance)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- MART_CHANNEL_ROI  (weekly grain, channel level)
-- ----------------------------------------------------------------------------
CREATE TABLE gold.mart_channel_roi (
    roi_sk                  BIGINT           NOT NULL IDENTITY(1,1),
    year_week               CHAR(8)          NOT NULL,   -- YYYY-WWW
    date_key                INT,
    channel_sk              INT,
    brand_sk                INT,
    -- Funnel metrics
    sessions                INT,
    applications            INT,
    approvals               INT,
    disbursements           INT,
    -- Financial metrics
    total_disbursed_zar     DECIMAL(18,2),
    total_collected_zar     DECIMAL(18,2),
    total_outstanding_zar   DECIMAL(18,2),
    total_written_off_zar   DECIMAL(18,2),
    campaign_spend_zar      DECIMAL(14,2),
    -- Rate metrics (computed in mart build)
    approval_rate           DECIMAL(8,6),
    funding_rate            DECIMAL(8,6),
    default_rate            DECIMAL(8,6),
    collection_rate         DECIMAL(8,6),
    cost_per_application    DECIMAL(10,2),
    cost_per_funded_loan    DECIMAL(10,2),
    -- Composite quality score
    channel_quality_score   DECIMAL(8,6),
    budget_recommendation   DECIMAL(14,2),
    CONSTRAINT pk_mart_roi PRIMARY KEY (roi_sk),
    CONSTRAINT fk_mr_channel FOREIGN KEY (channel_sk) REFERENCES gold.dim_channel (channel_sk),
    CONSTRAINT fk_mr_brand   FOREIGN KEY (brand_sk)   REFERENCES gold.dim_brand   (brand_sk)
);

-- ----------------------------------------------------------------------------
-- MART_CREDIT_RISK_SCORES  (ML model output — updated weekly)
-- ----------------------------------------------------------------------------
CREATE TABLE gold.mart_credit_risk_scores (
    score_sk                BIGINT           NOT NULL IDENTITY(1,1),
    application_id          VARCHAR(36)      NOT NULL,
    model_version           VARCHAR(20),
    score_date              DATE,
    default_probability     DECIMAL(8,6),
    risk_decile             TINYINT,
    risk_label              VARCHAR(20),     -- LOW_RISK | MEDIUM_RISK | HIGH_RISK
    recommended_action      VARCHAR(30),     -- AUTO_APPROVE | MANUAL_REVIEW | DECLINE
    model_auc               DECIMAL(6,4),
    CONSTRAINT pk_mart_scores PRIMARY KEY (score_sk)
);

-- ----------------------------------------------------------------------------
-- MART_AFFORDABILITY_SCORES  (ML regression output)
-- ----------------------------------------------------------------------------
CREATE TABLE gold.mart_affordability_scores (
    afford_sk               BIGINT           NOT NULL IDENTITY(1,1),
    application_id          VARCHAR(36)      NOT NULL,
    model_version           VARCHAR(20),
    score_date              DATE,
    predicted_disposable    DECIMAL(14,2),
    actual_disposable       DECIMAL(14,2),
    is_affordable           BIT,
    stress_scenario         VARCHAR(40),
    scenario_disposable     DECIMAL(14,2),
    CONSTRAINT pk_mart_afford PRIMARY KEY (afford_sk)
);

-- ============================================================================
-- INDEXES (covering indexes for common BI query patterns)
-- ============================================================================
CREATE INDEX ix_fact_app_datekey   ON gold.fact_loan_application (date_key);
CREATE INDEX ix_fact_app_customer  ON gold.fact_loan_application (customer_sk);
CREATE INDEX ix_fact_app_channel   ON gold.fact_loan_application (channel_sk);
CREATE INDEX ix_fact_repay_date    ON gold.fact_repayment (date_key);
CREATE INDEX ix_fact_repay_loan    ON gold.fact_repayment (loan_id);
CREATE INDEX ix_mart_roi_week      ON gold.mart_channel_roi (year_week, channel_sk);
CREATE INDEX ix_dim_cust_id        ON gold.dim_customer (customer_id) WHERE is_current = 1;
