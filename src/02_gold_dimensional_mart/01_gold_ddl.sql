-- =============================================================================
-- Phase 4: Gold Star Schema DDL & Delta Lake Optimizations
-- Target Catalog/Schema: retail_banking_dw.gold
-- Features: Liquid Clustering (CLUSTER BY), Deletion Vectors, Change Data Feed
-- =============================================================================

USE CATALOG retail_banking_dw;
USE SCHEMA gold;

-- 1. Dimension: Date (Role-playing dimension for transaction, origination, & snapshot dates)
CREATE TABLE IF NOT EXISTS retail_banking_dw.gold.dim_date (
    date_key INT NOT NULL COMMENT 'Surrogate key in YYYYMMDD format',
    full_date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    month_name STRING,
    day_of_month INT,
    is_weekend BOOLEAN,
    is_month_end BOOLEAN,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
)
USING DELTA
COMMENT 'Calendar dimension for loan portfolio and repayment analytics';

-- 2. Dimension: Customer (SCD Type 2 projected from silver_customers_scd2)
CREATE TABLE IF NOT EXISTS retail_banking_dw.gold.dim_customer (
    customer_sk BIGINT NOT NULL COMMENT 'Surrogate Key: xxhash64(customer_id, effective_from)',
    customer_id STRING NOT NULL COMMENT 'Party Natural Key',
    national_id STRING COMMENT 'Czech Birth Number (Rodne Cislo) - Masked via UC Policy',
    full_name STRING,
    email STRING COMMENT 'Customer Email - Masked via UC Policy',
    branch_region STRING NOT NULL COMMENT 'PRAGUE, BRNO, OSTRAVA - Filtered via UC Row Filter',
    banking_segment STRING NOT NULL COMMENT 'RETAIL, PREMIUM, PRIVATE',
    credit_score INT,
    credit_tier STRING NOT NULL COMMENT 'Historical credit tier (A, B, C, D) from SCD2',
    address_city STRING,
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)
)
USING DELTA
CLUSTER BY (customer_id, branch_region)
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'SCD Type 2 Customer Dimension with Liquid Clustering and CDF enabled';

-- 3. Dimension: Lending Product (Filtered to LENDING domain from silver_products)
CREATE TABLE IF NOT EXISTS retail_banking_dw.gold.dim_product (
    product_sk BIGINT NOT NULL COMMENT 'Surrogate Key: xxhash64(product_code)',
    product_code STRING NOT NULL COMMENT 'Product Natural Key',
    product_name STRING,
    product_domain STRING,
    product_category STRING COMMENT 'MORTGAGE, CONSUMER_LOAN, AUTO_LOAN',
    base_interest_rate DECIMAL(5,4),
    collateral_required BOOLEAN,
    CONSTRAINT pk_dim_product PRIMARY KEY (product_sk)
)
USING DELTA
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Lending Product Dimension';

-- 4. Fact: Loan Repayments (Transaction Grain Fact)
CREATE TABLE IF NOT EXISTS retail_banking_dw.gold.fact_loan_repayments (
    repayment_sk BIGINT NOT NULL COMMENT 'Surrogate Key: xxhash64(transaction_id)',
    transaction_id STRING NOT NULL COMMENT 'Degenerate Dimension - Core Transaction NK',
    contract_id STRING NOT NULL COMMENT 'Degenerate Dimension - Core Contract NK',
    transaction_date_key INT NOT NULL COMMENT 'FK to dim_date',
    customer_sk BIGINT NOT NULL COMMENT 'Point-in-time FK to dim_customer (SCD2)',
    product_sk BIGINT NOT NULL COMMENT 'FK to dim_product',
    transaction_type_code STRING NOT NULL COMMENT 'SCHEDULED_REPAYMENT, EARLY_REPAYMENT',
    payment_channel STRING,
    total_paid_amount DECIMAL(18,2),
    principal_paid_amount DECIMAL(18,2),
    interest_paid_amount DECIMAL(18,2),
    fee_paid_amount DECIMAL(18,2),
    days_past_due INT,
    _updated_at TIMESTAMP,
    CONSTRAINT pk_fact_loan_repayments PRIMARY KEY (repayment_sk)
)
USING DELTA
CLUSTER BY (transaction_date_key, customer_sk)
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Transaction-grain repayment fact table Liquid Clustered by date and customer';

-- 5. Fact: Monthly Contract Snapshot (Periodic Snapshot Fact for IFRS9 / Delinquency)
CREATE TABLE IF NOT EXISTS retail_banking_dw.gold.fact_monthly_contract_snapshot (
    snapshot_date_key INT NOT NULL COMMENT 'Month-end FK to dim_date (e.g., 20260131, 20260228, 20260331)',
    contract_id STRING NOT NULL COMMENT 'Contract Natural Key',
    customer_sk BIGINT NOT NULL COMMENT 'Point-in-time FK to dim_customer as of month-end',
    product_sk BIGINT NOT NULL COMMENT 'FK to dim_product',
    origination_date_key INT COMMENT 'FK to dim_date',
    principal_original_amount DECIMAL(18,2),
    remaining_principal_balance DECIMAL(18,2),
    cumulative_interest_paid DECIMAL(18,2),
    days_past_due INT NOT NULL,
    delinquency_bucket STRING NOT NULL COMMENT 'PERFORMING (0), EARLY_ARREARS (1-30), DELINQUENT (31-89), NPL_DEFAULT (90+)',
    contract_status STRING,
    _updated_at TIMESTAMP,
    CONSTRAINT pk_fact_monthly_snapshot PRIMARY KEY (snapshot_date_key, contract_id)
)
USING DELTA
CLUSTER BY (snapshot_date_key, delinquency_bucket)
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Periodic monthly snapshot fact tracking portfolio balances and delinquency buckets';