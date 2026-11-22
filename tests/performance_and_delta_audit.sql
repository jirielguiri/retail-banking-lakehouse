-- =============================================================================
-- Phase 7.2: Spark Query Tuning & Delta Lake Storage Audit Suite
-- Run individual blocks in Databricks SQL / Notebook to inspect execution plans
-- and Delta transaction log metadata during technical interviews.
-- =============================================================================

USE CATALOG retail_banking_dw;

-- -----------------------------------------------------------------------------
-- AUDIT 1: Verify BROADCAST & RANGE_JOIN in Spark Physical Plan
-- Look for: BroadcastHashJoin (dim_product, dim_date) and RangeJoin / binning
-- -----------------------------------------------------------------------------
EXPLAIN FORMATTED
SELECT /*+ BROADCAST(p), BROADCAST(d), RANGE_JOIN(c, 86400) */
    d.year,
    d.month_name,
    p.product_category,
    c.credit_tier,
    COUNT(f.repayment_sk) AS repayment_count,
    SUM(f.total_paid_amount) AS total_collected_czk
FROM retail_banking_dw.gold.fact_loan_repayments f
INNER JOIN retail_banking_dw.gold.dim_date d
    ON f.transaction_date_key = d.date_key
INNER JOIN retail_banking_dw.gold.dim_product p
    ON f.product_sk = p.product_sk
INNER JOIN retail_banking_dw.gold.dim_customer c
    ON f.customer_sk = c.customer_sk
GROUP BY d.year, d.month_name, p.product_category, c.credit_tier;

-- -----------------------------------------------------------------------------
-- AUDIT 2: Inspect Liquid Clustering, File Count, & Enabled Table Features
-- Verify: clusteringColumns, numFiles, sizeInBytes, tableFeatures
--         (deletionVectors, changeDataFeed, liquid)
-- -----------------------------------------------------------------------------
DESCRIBE DETAIL retail_banking_dw.gold.fact_loan_repayments;
DESCRIBE DETAIL retail_banking_dw.gold.fact_monthly_contract_snapshot;

-- -----------------------------------------------------------------------------
-- AUDIT 3: Inspect Delta Transaction Log (MERGE, OPTIMIZE, VACUUM Metrics)
-- Shows row-level Deletion Vector usage vs. full file rewrites
-- -----------------------------------------------------------------------------
SELECT
    version,
    timestamp,
    operation,
    operationMetrics
FROM (DESCRIBE HISTORY retail_banking_dw.gold.fact_loan_repayments)
ORDER BY version DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- AUDIT 4: Skew-Resilient Aggregation Pattern (Key Salting Reference)
-- Demonstrates how to handle a skewed corporate account generating 40%+ of rows
-- without causing OOM or straggler tasks during GROUP BY / JOIN shuffles.
-- -----------------------------------------------------------------------------
WITH salted_txns AS (
    SELECT
        contract_id,
        CAST(FLOOR(RAND() * 10) AS INT) AS salt_bucket,
        total_paid_amount
    FROM retail_banking_dw.gold.fact_loan_repayments
),
partial_agg AS (
    SELECT
        contract_id,
        salt_bucket,
        SUM(total_paid_amount) AS partial_paid_amount
    FROM salted_txns
    GROUP BY contract_id, salt_bucket
)
SELECT
    contract_id,
    SUM(partial_paid_amount) AS total_paid_amount
FROM partial_agg
GROUP BY contract_id
ORDER BY total_paid_amount DESC;