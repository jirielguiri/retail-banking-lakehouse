-- ============================================================================
-- Phase 2: Unity Catalog Setup (Catalog, Medallion Schemas, & Managed Volume)
-- Target Environment: Databricks Free Edition (Serverless / Unity Catalog)
-- ============================================================================

-- 1. Create the top-level Enterprise Data Warehouse Catalog
CREATE CATALOG IF NOT EXISTS retail_banking_dw
COMMENT 'Retail Banking & Loan Portfolio Lakehouse (FSLDM 3NF Silver + Kimball Gold)';

USE CATALOG retail_banking_dw;

-- 2. Create Medallion Architecture Schemas
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw ingestion layer populated via Auto Loader (cloud_files) with rescued data';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Integrated 3NF Core layer (Party, Contract, Financial Transaction) with SCD2 & Quarantine';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Dimensional Star Schema Mart for Loan Portfolio Performance & Delinquency Analytics';

-- 3. Create Unity Catalog Managed Volume for Raw Landing Files
CREATE VOLUME IF NOT EXISTS retail_banking_dw.bronze.landing
COMMENT 'Managed landing zone for daily JSON/CSV extracts from Core Banking';

-- 4. Verify Setup
SHOW SCHEMAS IN retail_banking_dw;
SHOW VOLUMES IN retail_banking_dw.bronze;