-- =============================================================================
-- Phase 7.2: Delta Lake Table Maintenance (OPTIMIZE, ANALYZE, VACUUM)
-- Target Catalog: retail_banking_dw (silver & gold)
-- Best Practice: Run on a scheduled weekly maintenance job, decoupled from
--                the daily ETL SLA window.
-- =============================================================================

USE CATALOG retail_banking_dw;

-- -----------------------------------------------------------------------------
-- 1. OPTIMIZE Liquid Clustered Gold Tables
--    Compacts small files and incrementally re-clusters data along CLUSTER BY
--    keys (no legacy ZORDER BY needed when Liquid Clustering is enabled).
-- -----------------------------------------------------------------------------
OPTIMIZE retail_banking_dw.gold.dim_customer;
OPTIMIZE retail_banking_dw.gold.fact_loan_repayments;
OPTIMIZE retail_banking_dw.gold.fact_monthly_contract_snapshot;

-- Note: For a full re-clustering rewrite after changing CLUSTER BY keys, use:
-- OPTIMIZE retail_banking_dw.gold.fact_loan_repayments FULL;

-- -----------------------------------------------------------------------------
-- 2. ANALYZE TABLE (Cost-Based Optimizer Statistics)
--    Refreshes table & column statistics (min, max, distinct count, nulls) so
--    Spark CBO chooses optimal broadcast thresholds and join order.
-- -----------------------------------------------------------------------------
ANALYZE TABLE retail_banking_dw.gold.dim_date COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE retail_banking_dw.gold.dim_product COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE retail_banking_dw.gold.dim_customer COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE retail_banking_dw.gold.fact_loan_repayments COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE retail_banking_dw.gold.fact_monthly_contract_snapshot COMPUTE STATISTICS FOR ALL COLUMNS;

-- -----------------------------------------------------------------------------
-- 3. VACUUM (Storage Cleanup with Safe 7-Day Time-Travel Retention)
--    Removes unreferenced Parquet files older than 168 hours (7 days).
--    NEVER lower retention below active streaming/reader durations in prod.
-- -----------------------------------------------------------------------------
VACUUM retail_banking_dw.gold.dim_customer RETAIN 168 HOURS;
VACUUM retail_banking_dw.gold.fact_loan_repayments RETAIN 168 HOURS;
VACUUM retail_banking_dw.gold.fact_monthly_contract_snapshot RETAIN 168 HOURS;