# Databricks notebook source
# ==============================================================================
# Phase 7.3: Regulatory CDF Outbox (Structured Streaming + foreachBatch)
# Source : retail_banking_dw.gold.fact_loan_repayments (Delta Change Data Feed)
# Target : retail_banking_dw.gold.regulatory_repayment_cdf_outbox
# Pattern: Incremental triggered streaming via trigger(availableNow=True)
# ==============================================================================

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CATALOG = "retail_banking_dw"
SOURCE_TABLE = f"{CATALOG}.gold.fact_loan_repayments"
TARGET_TABLE = f"{CATALOG}.gold.regulatory_repayment_cdf_outbox"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/bronze/landing/_checkpoints/regulatory_cdf_outbox"

# ------------------------------------------------------------------------------
# 1. Create the Immutable Regulatory Outbox Target Table (if not exists)
# ------------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    outbox_event_id BIGINT NOT NULL COMMENT 'Deterministic event key: xxhash64(transaction_id, commit_version, change_type)',
    transaction_id STRING NOT NULL COMMENT 'Core Banking Transaction NK',
    contract_id STRING NOT NULL COMMENT 'Core Banking Contract NK',
    transaction_date_key INT NOT NULL,
    transaction_type_code STRING NOT NULL,
    payment_channel STRING,
    total_paid_amount DECIMAL(18,2),
    principal_paid_amount DECIMAL(18,2),
    interest_paid_amount DECIMAL(18,2),
    fee_paid_amount DECIMAL(18,2),
    days_past_due INT,
    cdf_change_type STRING NOT NULL COMMENT 'insert, update_postimage, or delete (excludes update_preimage)',
    cdf_commit_version BIGINT NOT NULL COMMENT 'Source Delta table commit version',
    cdf_commit_timestamp TIMESTAMP NOT NULL COMMENT 'Source Delta table commit timestamp',
    published_at TIMESTAMP NOT NULL COMMENT 'Timestamp when event was published to outbox',
    CONSTRAINT pk_regulatory_cdf_outbox PRIMARY KEY (outbox_event_id)
)
USING DELTA
CLUSTER BY (cdf_commit_version, transaction_date_key)
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true'
)
COMMENT 'Incremental downstream regulatory & General Ledger outbox fed via Delta CDF streaming'
""")

# ------------------------------------------------------------------------------
# 2. Define Idempotent foreachBatch Micro-Batch Writer
#    Uses micro_batch_df.sparkSession so it runs cleanly on Serverless compute.
# ------------------------------------------------------------------------------
def publish_cdf_micro_batch(micro_batch_df: DataFrame, batch_id: int) -> None:
    batch_count = micro_batch_df.count()
    if batch_count == 0:
        print(f"[Batch {batch_id}] No qualifying CDF events to publish.")
        return

    batch_spark = micro_batch_df.sparkSession
    micro_batch_df.createOrReplaceTempView("v_regulatory_cdf_batch")

    batch_spark.sql(f"""
        MERGE INTO {TARGET_TABLE} AS tgt
        USING v_regulatory_cdf_batch AS src
           ON tgt.outbox_event_id = src.outbox_event_id
        WHEN NOT MATCHED THEN
            INSERT (
                outbox_event_id,
                transaction_id,
                contract_id,
                transaction_date_key,
                transaction_type_code,
                payment_channel,
                total_paid_amount,
                principal_paid_amount,
                interest_paid_amount,
                fee_paid_amount,
                days_past_due,
                cdf_change_type,
                cdf_commit_version,
                cdf_commit_timestamp,
                published_at
            )
            VALUES (
                src.outbox_event_id,
                src.transaction_id,
                src.contract_id,
                src.transaction_date_key,
                src.transaction_type_code,
                src.payment_channel,
                src.total_paid_amount,
                src.principal_paid_amount,
                src.interest_paid_amount,
                src.fee_paid_amount,
                src.days_past_due,
                src.cdf_change_type,
                src.cdf_commit_version,
                src.cdf_commit_timestamp,
                src.published_at
            )
    """)
    print(f"[Batch {batch_id}] Published {batch_count} CDF events to {TARGET_TABLE}.")

# ------------------------------------------------------------------------------
# 3. Read Delta Change Data Feed as a Stream & Execute with availableNow=True
# ------------------------------------------------------------------------------
cdf_stream_df = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table(SOURCE_TABLE)
    # Exclude 'update_preimage' so downstream consumers only receive final post-update states
    .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
    .select(
        F.xxhash64(
            F.col("transaction_id"),
            F.col("_commit_version"),
            F.col("_change_type"),
        ).alias("outbox_event_id"),
        F.col("transaction_id"),
        F.col("contract_id"),
        F.col("transaction_date_key"),
        F.col("transaction_type_code"),
        F.col("payment_channel"),
        F.col("total_paid_amount"),
        F.col("principal_paid_amount"),
        F.col("interest_paid_amount"),
        F.col("fee_paid_amount"),
        F.col("days_past_due"),
        F.col("_change_type").alias("cdf_change_type"),
        F.col("_commit_version").alias("cdf_commit_version"),
        F.col("_commit_timestamp").alias("cdf_commit_timestamp"),
        F.current_timestamp().alias("published_at"),
    )
)

query = (
    cdf_stream_df.writeStream
    .foreachBatch(publish_cdf_micro_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
print("Regulatory CDF Outbox incremental stream completed successfully.")