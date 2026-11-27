# Retail Banking & Loan Portfolio Lakehouse (Databricks)

[![Lakehouse CI/CD Pipeline](https://github.com/jirielguiri/retail-banking-lakehouse/actions/workflows/ci_cd.yml/badge.svg)](https://github.com/jirielguiri/retail-banking-lakehouse/actions/workflows/ci_cd.yml)

An end-to-end, enterprise-grade **Medallion Lakehouse** built on **Databricks Unity Catalog**, **Lakeflow Spark Declarative Pipelines (`@dp`)**, **Structured Streaming (Delta CDF)**, and **Delta Lake**, combining **FSLDM-inspired 3NF Core Data Modeling** with a **Kimball Dimensional Star Schema**.

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
| - _rescued_data    | ------> | - silver_quarantine_ |         | regulatory_       |
|                    |         |   records            |         |   repayment_cdf_  |
|                    |         |                      |         |   outbox (Stream) |
|                    |         |                      |         | Liquid Clustering |
|                    |         |                      |         | Deletion Vectors  |
+--------------------+         +----------------------+         +-------------------+
```

---

## Key Technical Highlights

* **Enterprise Financial Data Modeling (`docs/data_models/`):**
  * **Silver 3NF Core Model:** Inspired by the Financial Services Logical Data Model (FSLDM), separating Party/Customer, Product, Contract, and Financial Transaction entities with a centralized Quarantine audit table.
  * **Gold Kimball Star Schema:** Designed for Loan Portfolio Performance and IFRS 9 Delinquency Analytics (`PERFORMING`, `EARLY_ARREARS 1-30`, `DELINQUENT 31-89`, `NPL_DEFAULT 90+`), featuring **point-in-time SCD Type 2 joins** linking historical repayments to the exact customer credit tier active at the transaction timestamp.
  * Full Source-to-Target Mapping documented in [docs/sttm_mapping.md](docs/sttm_mapping.md).
* **Lakeflow Spark Declarative Pipelines (`src/01_sdp_bronze_silver/`):**
  * Incremental ingestion using **Auto Loader (`cloudFiles`)** with schema inference, schema evolution (`addNewColumns`), and `_rescued_data` capture.
  * Declarative data quality enforcement via `@dp.expect_or_drop` paired with explicit quarantine routing for malformed payloads.
  * Automated **SCD Type 2** history tracking on customer credit scores, credit tiers, segments, and cities using `dp.create_auto_cdc_flow()`.
* **Delta Lake Performance, Tuning & Maintenance (`src/02_gold_dimensional_mart/`):**
  * **Liquid Clustering (`CLUSTER BY`)**, **Deletion Vectors**, and **Change Data Feed (`delta.enableChangeDataFeed = true`)** enabled on Gold tables for low-latency `MERGE INTO` operations.
  * Explicit Spark join tuning using `/*+ RANGE_JOIN(dc, 86400), BROADCAST(dp) */` on point-in-time SCD Type 2 fact lookups, verified via `EXPLAIN FORMATTED` and `DESCRIBE HISTORY` in `tests/performance_and_delta_audit.sql`.
  * Decoupled weekly Delta maintenance (`OPTIMIZE`, `ANALYZE TABLE ... FOR ALL COLUMNS`, and `VACUUM RETAIN 168 HOURS`) scheduled separately from daily ETL SLAs.
* **Incremental Regulatory CDF Outbox (`src/04_streaming_cdf/`):**
  * Structured Streaming pipeline consuming Delta Change Data Feed (`readChangeFeed = true`) from `gold.fact_loan_repayments` via `trigger(availableNow=True)` and Unity Catalog Volume checkpoints.
  * Filters out `update_preimage` and writes idempotently into `gold.regulatory_repayment_cdf_outbox` using `foreachBatch` + deterministic `xxhash64` `MERGE INTO`.
* **Unity Catalog Security & Compliance (`src/03_uc_governance/`):**
  * **Column-Level PII Masking:** SQL UDF masks protecting Czech national IDs (`XXXXXX/1234`), emails (`j***@email.cz`), and phone numbers (`+420******222`) for non-compliance roles.
  * **Row-Level Security (RLS):** Branch region row filter isolating `PRAGUE`, `BRNO`, and `OSTRAVA` customer records dynamically at query time.
* **DevOps, Testing & CI/CD (`.github/workflows/`, `databricks.yml`, `tests/unit/`):**
  * Pure PySpark business rules decoupled into `src/utils/transformations.py` and verified with local-mode **`pytest`** unit tests and **`ruff`** linting on every push and pull request via GitHub Actions.
  * Packaged as a **Databricks Asset Bundle (DAB)** with `dev`/`prod` targets, `dbutils.jobs.taskValues` runtime metric handoffs, and conditional `condition_task` branching.

---

## Data Models

### Silver Layer: 3NF Core Model
![Silver 3NF Core Model](docs/data_models/Silver%20Layer_%203NF%20Core%20Model%20(DBML).png)

### Gold Layer: Kimball Star Schema
![Gold Star Schema](docs/data_models/Gold%20Star%20Schema.png)

---

## Repository Structure

```text
retail-banking-lakehouse/
├── .github/
│   └── workflows/
│       └── ci_cd.yml                               # GitHub Actions: Ruff, PySpark pytest, & DAB CI/CD
├── databricks.yml                                  # Databricks Asset Bundle (daily ETL + weekly maintenance)
├── requirements-dev.txt                            # CI/CD Python test & lint dependencies
├── README.md                                       # Architecture & deployment documentation
├── docs/
│   ├── sttm_mapping.md                             # End-to-end Source-to-Target Mapping
│   └── data_models/
│       ├── silver_3nf_core.dbml                    # Silver 3NF DBML schema definition
│       ├── gold_star_schema.dbml                   # Gold Star Schema DBML definition
│       ├── Silver Layer_ 3NF Core Model (DBML).png # Visual ER diagram (Silver)
│       └── Gold Star Schema.png                    # Visual ER diagram (Gold)
├── src/
│   ├── 00_setup_and_generator/
│   │   ├── 01_uc_setup.sql                         # Catalog, schemas, & managed volume DDL
│   │   └── 02_synthetic_data_gen.py                # Synthetic Czech retail banking data generator
│   ├── 01_sdp_bronze_silver/
│   │   └── bronze_to_silver_sdp.py                 # Lakeflow Declarative Pipeline (@dp + AUTO CDC)
│   ├── 02_gold_dimensional_mart/
│   │   ├── 01_gold_ddl.sql                         # Gold Star Schema DDL (Liquid Clustering + CDF)
│   │   ├── 02_gold_etl_merge.py                    # Idempotent MERGE INTO & tuned SCD2 fact ETL
│   │   ├── 03_pipeline_audit_task.py               # Publishes quarantine/row metrics via taskValues
│   │   └── 04_delta_maintenance.sql                # Weekly OPTIMIZE, ANALYZE TABLE, & VACUUM
│   ├── 03_uc_governance/
│   │   └── uc_security_policies.sql                # Unity Catalog Column Masks & Row Filter UDFs
│   ├── 04_streaming_cdf/
│   │   └── regulatory_cdf_outbox_stream.py         # Structured Streaming CDF outbox (foreachBatch)
│   └── utils/
│       └── transformations.py                      # Pure PySpark business rules (unit-tested)
└── tests/
    ├── dq_and_security_validation.sql              # End-to-end DQ, SCD2, CDF, & RLS audit suite
    ├── performance_and_delta_audit.sql             # EXPLAIN, DESCRIBE DETAIL/HISTORY, & salting
    └── unit/
        └── test_transformations.py                 # PySpark pytest boundary tests
```

---

## Quickstart & Deployment

1. **Provision Unity Catalog & Generate Landing Data:** Run `src/00_setup_and_generator/02_synthetic_data_gen.py` to initialize `retail_banking_dw` (`bronze`, `silver`, `gold`), create `/Volumes/retail_banking_dw/bronze/landing`, and write Day 1 & Day 2 JSON/CSV batches.
2. **Run the Lakeflow Declarative Pipeline:** Execute the pipeline pointing to `src/01_sdp_bronze_silver/bronze_to_silver_sdp.py` (catalog `retail_banking_dw`, default schema `silver`) to populate Bronze streaming tables, Silver 3NF tables, `silver_quarantine_records`, and `silver_customers_scd2`.
3. **Build the Gold Dimensional Mart & Regulatory Outbox:** Run `src/02_gold_dimensional_mart/02_gold_etl_merge.py` followed by `src/04_streaming_cdf/regulatory_cdf_outbox_stream.py`.
4. **Apply Governance Policies & Run Audit Tests:** Execute `src/03_uc_governance/uc_security_policies.sql`, then verify results with `tests/dq_and_security_validation.sql` and `tests/performance_and_delta_audit.sql`.
5. **Run Local Unit Tests & Bundle Validation:**
   ```bash
   pytest tests/unit -v
   databricks bundle validate -t dev
   ```