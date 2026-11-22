# Databricks notebook source
# ==============================================================================
# Phase 7: Post-ETL Audit Task — Publishes Runtime Metrics via taskValues
# Downstream DAB condition_task reads `quarantine_count` to trigger alerts.
# ==============================================================================

CATALOG = "retail_banking_dw"

quarantine_count = spark.table(f"{CATALOG}.silver.silver_quarantine_records").count()
gold_repayments_count = spark.table(f"{CATALOG}.gold.fact_loan_repayments").count()
gold_snapshots_count = spark.table(f"{CATALOG}.gold.fact_monthly_contract_snapshot").count()

print(f"Quarantined Records : {quarantine_count}")
print(f"Gold Repayments     : {gold_repayments_count}")
print(f"Gold Snapshots      : {gold_snapshots_count}")

# Publish metrics to Databricks Workflow taskValues for downstream branching
dbutils.jobs.taskValues.set(key="quarantine_count", value=int(quarantine_count))
dbutils.jobs.taskValues.set(key="gold_repayments_count", value=int(gold_repayments_count))
dbutils.jobs.taskValues.set(key="gold_snapshots_count", value=int(gold_snapshots_count))