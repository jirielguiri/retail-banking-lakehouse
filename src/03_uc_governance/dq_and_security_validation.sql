-- =============================================================================
-- Phase 5: End-to-End Portfolio Verification & Audit Queries
-- Validates Quarantine, SCD2 History, Point-in-Time Fact Joins, CDF, & UC Security
-- =============================================================================

USE CATALOG retail_banking_dw;

-- -----------------------------------------------------------------------------
-- TEST 1: Verify Silver Quarantine Routing & Auto Loader Rescued Data Capture
-- Expected: 3 quarantined records (CUSTOMERS null PK / INVALID_SCORE in _rescued_data,
--           CONTRACTS negative principal CNTR-5004, TRANSACTIONS negative payment TXN-9999)
-- -----------------------------------------------------------------------------
SELECT
    source_entity,
    failed_rule,
    raw_payload,
    quarantined_at
FROM retail_banking_dw.silver.silver_quarantine_records
ORDER BY source_entity;

-- -----------------------------------------------------------------------------
-- TEST 2: Verify SCD Type 2 History Tracking on Customers (AUTO CDC) & PII Masking
-- Expected: CUST-1001 (Jan Novak) has 2 rows:
--   1) 2026-01-10 to 2026-02-15: RETAIL, Score 680 (Tier B), Praha, is_current = false
--   2) 2026-02-15 to NULL:       PREMIUM, Score 765 (Tier A), Jesenice, is_current = true
--   Also verify national_id ("XXXXXX/1234") and email ("j***@email.cz") are masked!
-- -----------------------------------------------------------------------------
SELECT
    customer_sk,
    customer_id,
    full_name,
    national_id,
    email,
    branch_region,
    banking_segment,
    credit_score,
    credit_tier,
    address_city,
    effective_from,
    effective_to,
    is_current
FROM retail_banking_dw.gold.dim_customer
WHERE customer_id IN ('CUST-1001', 'CUST-1002', 'CUST-1004')
ORDER BY customer_id, effective_from;

-- -----------------------------------------------------------------------------
-- TEST 3: Verify Point-in-Time SCD Type 2 Join in fact_loan_repayments
-- Expected: Contract CNTR-5001 (Jan Novak) has two repayments linked to DIFFERENT
--           customer_sk surrogate keys based on transaction_date:
--   - TXN-9001 (2026-02-01) -> Joined to Tier B / RETAIL / Praha version
--   - TXN-9002 (2026-03-01) -> Joined to Tier A / PREMIUM / Jesenice version
-- -----------------------------------------------------------------------------
SELECT
    f.transaction_id,
    f.contract_id,
    f.transaction_date_key,
    f.transaction_type_code,
    f.total_paid_amount,
    c.customer_sk,
    c.full_name,
    c.banking_segment AS segment_at_time_of_payment,
    c.credit_tier AS tier_at_time_of_payment,
    c.address_city AS city_at_time_of_payment
FROM retail_banking_dw.gold.fact_loan_repayments f
INNER JOIN retail_banking_dw.gold.dim_customer c
    ON f.customer_sk = c.customer_sk
WHERE f.contract_id = 'CNTR-5001'
ORDER BY f.transaction_date_key;

-- -----------------------------------------------------------------------------
-- TEST 4: Verify Unity Catalog Row-Level Security (RLS) Filter on dim_customer
-- Expected: Only PRAGUE and BRNO regions appear; OSTRAVA rows are filtered out
--           automatically by retail_banking_dw.gold.filter_branch_region.
-- -----------------------------------------------------------------------------
SELECT
    branch_region,
    banking_segment,
    COUNT(*) AS visible_customer_versions
FROM retail_banking_dw.gold.dim_customer
GROUP BY branch_region, banking_segment
ORDER BY branch_region, banking_segment;

-- -----------------------------------------------------------------------------
-- TEST 5: Portfolio Delinquency & IFRS9 Bucket Analytics (Star Schema Executive Query)
-- Expected: Monthly portfolio breakdown across PERFORMING, DELINQUENT (31-89), and NPL (90+)
-- -----------------------------------------------------------------------------
SELECT
    s.snapshot_date_key,
    p.product_category,
    s.delinquency_bucket,
    COUNT(DISTINCT s.contract_id) AS contract_count,
    SUM(s.remaining_principal_balance) AS total_remaining_exposure_czk,
    SUM(s.cumulative_interest_paid) AS cumulative_interest_revenue_czk
FROM retail_banking_dw.gold.fact_monthly_contract_snapshot s
INNER JOIN retail_banking_dw.gold.dim_product p
    ON s.product_sk = p.product_sk
GROUP BY s.snapshot_date_key, p.product_category, s.delinquency_bucket
ORDER BY s.snapshot_date_key, p.product_category, s.delinquency_bucket;

-- -----------------------------------------------------------------------------
-- TEST 6: Verify Delta Change Data Feed (CDF) & Liquid Clustering Metadata
-- -----------------------------------------------------------------------------
SELECT
    customer_id,
    credit_tier,
    banking_segment,
    _change_type,
    _commit_version,
    _commit_timestamp
FROM table_changes('retail_banking_dw.gold.dim_customer', 1)
WHERE customer_id = 'CUST-1001'
ORDER BY _commit_version;

DESCRIBE DETAIL retail_banking_dw.gold.fact_loan_repayments;