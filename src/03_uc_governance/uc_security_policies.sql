-- =============================================================================
-- Phase 5: Unity Catalog Security & Governance (Column Masks & Row Filters)
-- Target Catalog: retail_banking_dw (silver & gold schemas)
-- =============================================================================

USE CATALOG retail_banking_dw;
USE SCHEMA gold;

-- -----------------------------------------------------------------------------
-- 0. Reset Existing Policies (Ensures Idempotent Re-runs before dropping UDFs)
-- -----------------------------------------------------------------------------
ALTER TABLE retail_banking_dw.gold.dim_customer DROP ROW FILTER;
ALTER TABLE retail_banking_dw.gold.dim_customer ALTER COLUMN national_id DROP MASK;
ALTER TABLE retail_banking_dw.gold.dim_customer ALTER COLUMN email DROP MASK;
ALTER TABLE retail_banking_dw.silver.silver_customers_scd2 ALTER COLUMN national_id DROP MASK;
ALTER TABLE retail_banking_dw.silver.silver_customers_scd2 ALTER COLUMN email DROP MASK;
ALTER TABLE retail_banking_dw.silver.silver_customers_scd2 ALTER COLUMN phone_number DROP MASK;

-- -----------------------------------------------------------------------------
-- 1. Create PII Column Masking SQL UDFs
-- -----------------------------------------------------------------------------

-- Mask Czech Birth Number (Rodne Cislo), e.g., "850115/1234" -> "XXXXXX/1234"
CREATE OR REPLACE FUNCTION retail_banking_dw.gold.mask_national_id(national_id STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('compliance_pii_admins') THEN national_id
    WHEN national_id IS NULL THEN NULL
    ELSE CONCAT('XXXXXX/', RIGHT(national_id, 4))
  END;

-- Mask Customer Email, e.g., "jan.novak@email.cz" -> "j***@email.cz"
CREATE OR REPLACE FUNCTION retail_banking_dw.gold.mask_email(email STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('compliance_pii_admins') THEN email
    WHEN email IS NULL THEN NULL
    ELSE regexp_replace(email, '(^.)([^@]+)(@.*$)', '$1***$3')
  END;

-- Mask Phone Number, e.g., "+420602111222" -> "+420******222"
CREATE OR REPLACE FUNCTION retail_banking_dw.gold.mask_phone(phone_number STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('compliance_pii_admins') THEN phone_number
    WHEN phone_number IS NULL THEN NULL
    ELSE CONCAT('+420******', RIGHT(phone_number, 3))
  END;

-- -----------------------------------------------------------------------------
-- 2. Apply Column Masks to Silver 3NF & Gold Dimensional Customer Tables
-- -----------------------------------------------------------------------------
ALTER TABLE retail_banking_dw.gold.dim_customer
  ALTER COLUMN national_id SET MASK retail_banking_dw.gold.mask_national_id;

ALTER TABLE retail_banking_dw.gold.dim_customer
  ALTER COLUMN email SET MASK retail_banking_dw.gold.mask_email;

ALTER TABLE retail_banking_dw.silver.silver_customers_scd2
  ALTER COLUMN national_id SET MASK retail_banking_dw.gold.mask_national_id;

ALTER TABLE retail_banking_dw.silver.silver_customers_scd2
  ALTER COLUMN email SET MASK retail_banking_dw.gold.mask_email;

ALTER TABLE retail_banking_dw.silver.silver_customers_scd2
  ALTER COLUMN phone_number SET MASK retail_banking_dw.gold.mask_phone;

-- -----------------------------------------------------------------------------
-- 3. Create & Apply Regional Row-Level Security (RLS) Filter UDF
--    Demo fallback for single-user Databricks Free Edition workspace:
--    non-group members default to PRAGUE and BRNO (hiding OSTRAVA) so the
--    Row Filter effect is immediately visible in validation queries.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION retail_banking_dw.gold.filter_branch_region(branch_region STRING, banking_segment STRING)
RETURNS BOOLEAN
RETURN
  CASE
    WHEN is_account_group_member('hq_risk_analysts') THEN TRUE
    WHEN is_account_group_member('branch_prague_analysts') THEN branch_region = 'PRAGUE'
    WHEN is_account_group_member('branch_brno_analysts') THEN branch_region = 'BRNO'
    WHEN is_account_group_member('branch_ostrava_analysts') THEN branch_region = 'OSTRAVA'
    WHEN is_account_group_member('retail_segment_analysts') THEN banking_segment = 'RETAIL'
    ELSE branch_region IN ('PRAGUE', 'BRNO')
  END;

ALTER TABLE retail_banking_dw.gold.dim_customer
  SET ROW FILTER retail_banking_dw.gold.filter_branch_region ON (branch_region, banking_segment);