# Retail Banking & Loan Portfolio Lakehouse (Databricks)

An end-to-end, enterprise-grade **Medallion Lakehouse** built on **Databricks Unity Catalog**, **Lakeflow Spark Declarative Pipelines (`@dp`)**, and **Delta Lake**, combining **FSLDM-inspired 3NF Core Data Modeling** with a **Kimball Dimensional Star Schema**.

---

## Architecture Overview

```text
+-----------------------------------------------------------------------------------+
|                  UNITY CATALOG: retail_banking_dw (Governance)                    |
|  - Managed Volume: /Volumes/retail_banking_dw/bronze/landing                      |
|  - Dynamic PII Column Masks (Czech Birth Number / Rodne Cislo, Email, Phone)      |
|  - Row-Level Security (RLS) Branch Filtering (PRAGUE, BRNO, OSTRAVA)              |
+-----------------------------------------------------------------------------------+
         |                                |                                |
         v                                v                                v
+--------------------+         +----------------------+         +-------------------+
|   BRONZE LAYER     |         |     SILVER LAYER     |         |    GOLD LAYER     |
|  (Raw Ingestion)   |         |  (3NF Core + SCD2)   |         |  (Star Schema)    |
+--------------------+         +----------------------+         +-------------------+
| Auto Loader        | ------> | @dp Expectations     | ------> | dim_date          |
| (cloudFiles)       |         | (@dp.expect_or_drop) |         | dim_customer(SCD2)|
| - customers (JSON) |         | - silver_products    |         | dim_product       |
| - products (JSON)  |         | - silver_contracts   |         | fact_loan_        |
| - contracts (CSV)  |         | - silver_financial_  |         |   repayments      |
| - transactions     |         |   transactions       |         | fact_monthly_     |
|   (JSON + Schema   |         | - silver_customers_  |         |   contract_       |
|    Evolution)      |         |   scd2 (AUTO CDC)    |         |   snapshot        |
| - _rescued_data    | ------> | - silver_quarantine_ |         | Liquid Clustering |
|                    |         |   records            |         | Deletion Vectors  |
+--------------------+         +----------------------+         +-------------------+