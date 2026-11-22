"""Pure PySpark transformation functions for Retail Banking Lakehouse.

Decoupled from Lakeflow SDP (@dp) and notebook I/O so business rules
can be unit-tested with pytest in CI/CD before deployment.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def add_credit_tier(df: DataFrame, score_col: str = "credit_score") -> DataFrame:
    """Derive standardized credit tier (A, B, C, D) from numeric credit score."""
    score = F.col(score_col).cast("int")
    return df.withColumn(score_col, score).withColumn(
        "credit_tier",
        F.when(score >= 750, F.lit("A"))
        .when(score >= 650, F.lit("B"))
        .when(score >= 550, F.lit("C"))
        .otherwise(F.lit("D")),
    )


def add_delinquency_bucket(df: DataFrame, dpd_col: str = "days_past_due") -> DataFrame:
    """Classify Days Past Due (DPD) into Basel / IFRS 9 delinquency buckets."""
    dpd = F.coalesce(F.col(dpd_col).cast("int"), F.lit(0))
    return df.withColumn(dpd_col, dpd).withColumn(
        "delinquency_bucket",
        F.when(dpd <= 0, F.lit("PERFORMING"))
        .when(dpd <= 30, F.lit("EARLY_ARREARS"))
        .when(dpd <= 89, F.lit("DELINQUENT"))
        .otherwise(F.lit("NPL_DEFAULT")),
    )


def add_total_transaction_amount(df: DataFrame) -> DataFrame:
    """Compute total_transaction_amount = principal + interest + coalesce(fee, 0)."""
    principal = F.col("principal_amount").cast(DecimalType(18, 2))
    interest = F.col("interest_amount").cast(DecimalType(18, 2))
    fee = F.coalesce(F.col("fee_amount").cast(DecimalType(18, 2)), F.lit(0.00)).cast(
        DecimalType(18, 2)
    )
    return (
        df.withColumn("principal_amount", principal)
        .withColumn("interest_amount", interest)
        .withColumn("fee_amount", fee)
        .withColumn(
            "total_transaction_amount",
            (principal + interest + fee).cast(DecimalType(18, 2)),
        )
    )