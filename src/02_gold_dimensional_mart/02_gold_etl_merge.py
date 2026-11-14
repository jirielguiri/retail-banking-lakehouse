# Databricks notebook source
# =============================================================================
# Phase 4: Gold Dimensional Mart ETL (Idempotent MERGE INTO & Point-in-Time SCD2)
# Target Catalog/Schema: retail_banking_dw.gold
# =============================================================================

spark.sql("USE CATALOG retail_banking_dw")
spark.sql("USE SCHEMA gold")

# -----------------------------------------------------------------------------
# 0. Ensure Gold Tables Exist with Liquid Clustering, Deletion Vectors & CDF
# -----------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS retail_banking_dw.gold.dim_date (
    date_key INT NOT NULL,
    full_date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    month_name STRING,
    day_of_month INT,
    is_weekend BOOLEAN,
    is_month_end BOOLEAN,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS retail_banking_dw.gold.dim_customer (
    customer_sk BIGINT NOT NULL,
    customer_id STRING NOT NULL,
    national_id STRING,
    full_name STRING,
    email STRING,
    branch_region STRING NOT NULL,
    banking_segment STRING NOT NULL,
    credit_score INT,
    credit_tier STRING NOT NULL,
    address_city STRING,
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)
) USING DELTA
CLUSTER BY (customer_id, branch_region)
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableChangeDataFeed' = 'true')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS retail_banking_dw.gold.dim_product (
    product_sk BIGINT NOT NULL,
    product_code STRING NOT NULL,
    product_name STRING,
    product_domain STRING,
    product_category STRING,
    base_interest_rate DECIMAL(5,4),
    collateral_required BOOLEAN,
    CONSTRAINT pk_dim_product PRIMARY KEY (product_sk)
) USING DELTA
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableChangeDataFeed' = 'true')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS retail_banking_dw.gold.fact_loan_repayments (
    repayment_sk BIGINT NOT NULL,
    transaction_id STRING NOT NULL,
    contract_id STRING NOT NULL,
    transaction_date_key INT NOT NULL,
    customer_sk BIGINT NOT NULL,
    product_sk BIGINT NOT NULL,
    transaction_type_code STRING NOT NULL,
    payment_channel STRING,
    total_paid_amount DECIMAL(18,2),
    principal_paid_amount DECIMAL(18,2),
    interest_paid_amount DECIMAL(18,2),
    fee_paid_amount DECIMAL(18,2),
    days_past_due INT,
    _updated_at TIMESTAMP,
    CONSTRAINT pk_fact_loan_repayments PRIMARY KEY (repayment_sk)
) USING DELTA
CLUSTER BY (transaction_date_key, customer_sk)
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableChangeDataFeed' = 'true')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS retail_banking_dw.gold.fact_monthly_contract_snapshot (
    snapshot_date_key INT NOT NULL,
    contract_id STRING NOT NULL,
    customer_sk BIGINT NOT NULL,
    product_sk BIGINT NOT NULL,
    origination_date_key INT,
    principal_original_amount DECIMAL(18,2),
    remaining_principal_balance DECIMAL(18,2),
    cumulative_interest_paid DECIMAL(18,2),
    days_past_due INT NOT NULL,
    delinquency_bucket STRING NOT NULL,
    contract_status STRING,
    _updated_at TIMESTAMP,
    CONSTRAINT pk_fact_monthly_snapshot PRIMARY KEY (snapshot_date_key, contract_id)
) USING DELTA
CLUSTER BY (snapshot_date_key, delinquency_bucket)
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableChangeDataFeed' = 'true')
""")

# -----------------------------------------------------------------------------
# 1. Populate dim_date (2025-01-01 to 2030-12-31) via MERGE INTO
# -----------------------------------------------------------------------------
spark.sql("""
MERGE INTO retail_banking_dw.gold.dim_date AS tgt
USING (
    SELECT
        CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
        d AS full_date,
        year(d) AS year,
        quarter(d) AS quarter,
        month(d) AS month,
        date_format(d, 'MMMM') AS month_name,
        dayofmonth(d) AS day_of_month,
        dayofweek(d) IN (1, 7) AS is_weekend,
        d = last_day(d) AS is_month_end
    FROM (
        SELECT explode(sequence(to_date('2025-01-01'), to_date('2030-12-31'), interval 1 day)) AS d
    )
) AS src
ON tgt.date_key = src.date_key
WHEN NOT MATCHED THEN INSERT *
""")
print("1. dim_date merged successfully.")

# -----------------------------------------------------------------------------
# 2. Populate dim_customer from silver_customers_scd2 (SCD Type 2)
# -----------------------------------------------------------------------------
spark.sql("""
MERGE INTO retail_banking_dw.gold.dim_customer AS tgt
USING (
    SELECT
        xxhash64(customer_id, `__START_AT`) AS customer_sk,
        customer_id,
        national_id,
        concat_ws(' ', first_name, last_name) AS full_name,
        email,
        branch_region,
        banking_segment,
        credit_score,
        credit_tier,
        address_city,
        `__START_AT` AS effective_from,
        `__END_AT` AS effective_to,
        (`__END_AT` IS NULL) AS is_current
    FROM retail_banking_dw.silver.silver_customers_scd2
) AS src
ON tgt.customer_sk = src.customer_sk
WHEN MATCHED AND (
    tgt.is_current <> src.is_current
    OR tgt.effective_to IS DISTINCT FROM src.effective_to
) THEN UPDATE SET
    tgt.effective_to = src.effective_to,
    tgt.is_current = src.is_current
WHEN NOT MATCHED THEN INSERT *
""")
print("2. dim_customer (SCD2) merged successfully.")

# -----------------------------------------------------------------------------
# 3. Populate dim_product (Filtered to LENDING domain)
# -----------------------------------------------------------------------------
spark.sql("""
MERGE INTO retail_banking_dw.gold.dim_product AS tgt
USING (
    SELECT
        xxhash64(product_code) AS product_sk,
        product_code,
        product_name,
        product_domain,
        product_category,
        base_interest_rate,
        collateral_required
    FROM retail_banking_dw.silver.silver_products
    WHERE product_domain = 'LENDING'
) AS src
ON tgt.product_sk = src.product_sk
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print("3. dim_product merged successfully.")

# -----------------------------------------------------------------------------
# 4. Populate fact_loan_repayments with Point-in-Time SCD2 Customer Join
# -----------------------------------------------------------------------------
spark.sql("""
MERGE INTO retail_banking_dw.gold.fact_loan_repayments AS tgt
USING (
    WITH earliest_customer_versions AS (
        SELECT customer_id, MIN(effective_from) AS min_effective_from
        FROM retail_banking_dw.gold.dim_customer
        GROUP BY customer_id
    )
    SELECT
        xxhash64(t.transaction_id) AS repayment_sk,
        t.transaction_id,
        t.contract_id,
        CAST(date_format(t.transaction_date, 'yyyyMMdd') AS INT) AS transaction_date_key,
        dc.customer_sk,
        dp.product_sk,
        t.transaction_type_code,
        t.payment_channel,
        t.total_transaction_amount AS total_paid_amount,
        t.principal_amount AS principal_paid_amount,
        t.interest_amount AS interest_paid_amount,
        t.fee_amount AS fee_paid_amount,
        t.days_past_due,
        current_timestamp() AS _updated_at
    FROM retail_banking_dw.silver.silver_financial_transactions t
    INNER JOIN retail_banking_dw.silver.silver_contracts c
        ON t.contract_id = c.contract_id
    INNER JOIN retail_banking_dw.gold.dim_product dp
        ON c.product_code = dp.product_code
    INNER JOIN earliest_customer_versions ecv
        ON c.customer_id = ecv.customer_id
    -- Point-in-time SCD Type 2 join with precomputed earliest version fallback
    INNER JOIN retail_banking_dw.gold.dim_customer dc
        ON c.customer_id = dc.customer_id
        AND (
            (t.transaction_timestamp >= dc.effective_from AND (t.transaction_timestamp < dc.effective_to OR dc.effective_to IS NULL))
            OR (t.transaction_timestamp < dc.effective_from AND dc.effective_from = ecv.min_effective_from)
        )
    WHERE t.transaction_type_code IN ('SCHEDULED_REPAYMENT', 'EARLY_REPAYMENT')
) AS src
ON tgt.repayment_sk = src.repayment_sk
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print("4. fact_loan_repayments merged successfully (with point-in-time SCD2 join).")

# -----------------------------------------------------------------------------
# 5. Populate fact_monthly_contract_snapshot (As of Jan 31, Feb 28, Mar 31, 2026)
# -----------------------------------------------------------------------------
spark.sql("""
MERGE INTO retail_banking_dw.gold.fact_monthly_contract_snapshot AS tgt
USING (
    WITH snapshot_months AS (
        SELECT explode(array(
            to_date('2026-01-31'),
            to_date('2026-02-28'),
            to_date('2026-03-31')
        )) AS snap_date
    ),
    earliest_customer_versions AS (
        SELECT customer_id, MIN(effective_from) AS min_effective_from
        FROM retail_banking_dw.gold.dim_customer
        GROUP BY customer_id
    ),
    txns_to_date AS (
        SELECT
            m.snap_date,
            c.contract_id,
            COALESCE(SUM(CASE WHEN t.transaction_type_code IN ('SCHEDULED_REPAYMENT', 'EARLY_REPAYMENT') THEN t.principal_amount ELSE 0 END), 0) AS cum_principal_paid,
            COALESCE(SUM(CASE WHEN t.transaction_type_code IN ('SCHEDULED_REPAYMENT', 'EARLY_REPAYMENT') THEN t.interest_amount ELSE 0 END), 0) AS cum_interest_paid,
            COALESCE(MAX(t.days_past_due), 0) AS max_dpd
        FROM snapshot_months m
        CROSS JOIN retail_banking_dw.silver.silver_contracts c
        LEFT JOIN retail_banking_dw.silver.silver_financial_transactions t
            ON c.contract_id = t.contract_id
            AND t.transaction_date <= m.snap_date
        WHERE c.origination_date <= m.snap_date
        GROUP BY m.snap_date, c.contract_id
    )
    SELECT
        CAST(date_format(tx.snap_date, 'yyyyMMdd') AS INT) AS snapshot_date_key,
        c.contract_id,
        dc.customer_sk,
        dp.product_sk,
        CAST(date_format(c.origination_date, 'yyyyMMdd') AS INT) AS origination_date_key,
        c.principal_amount AS principal_original_amount,
        CAST(GREATEST(c.principal_amount - tx.cum_principal_paid, 0.00) AS DECIMAL(18,2)) AS remaining_principal_balance,
        CAST(tx.cum_interest_paid AS DECIMAL(18,2)) AS cumulative_interest_paid,
        tx.max_dpd AS days_past_due,
        CASE
            WHEN tx.max_dpd = 0 THEN 'PERFORMING (0)'
            WHEN tx.max_dpd BETWEEN 1 AND 30 THEN 'EARLY_ARREARS (1-30)'
            WHEN tx.max_dpd BETWEEN 31 AND 89 THEN 'DELINQUENT (31-89)'
            ELSE 'NPL_DEFAULT (90+)'
        END AS delinquency_bucket,
        c.contract_status,
        current_timestamp() AS _updated_at
    FROM txns_to_date tx
    INNER JOIN retail_banking_dw.silver.silver_contracts c
        ON tx.contract_id = c.contract_id
    INNER JOIN retail_banking_dw.gold.dim_product dp
        ON c.product_code = dp.product_code
    INNER JOIN earliest_customer_versions ecv
        ON c.customer_id = ecv.customer_id
    INNER JOIN retail_banking_dw.gold.dim_customer dc
        ON c.customer_id = dc.customer_id
        AND (
            (CAST(tx.snap_date AS TIMESTAMP) >= dc.effective_from AND (CAST(tx.snap_date AS TIMESTAMP) < dc.effective_to OR dc.effective_to IS NULL))
            OR (CAST(tx.snap_date AS TIMESTAMP) < dc.effective_from AND dc.effective_from = ecv.min_effective_from)
        )
) AS src
ON tgt.snapshot_date_key = src.snapshot_date_key AND tgt.contract_id = src.contract_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print("5. fact_monthly_contract_snapshot merged successfully.")

# -----------------------------------------------------------------------------
# 6. Trigger Liquid Clustering Compaction (OPTIMIZE)
# -----------------------------------------------------------------------------
spark.sql("OPTIMIZE retail_banking_dw.gold.dim_customer")
spark.sql("OPTIMIZE retail_banking_dw.gold.fact_loan_repayments")
spark.sql("OPTIMIZE retail_banking_dw.gold.fact_monthly_contract_snapshot")
print("6. Liquid Clustering OPTIMIZE completed on Gold tables!")