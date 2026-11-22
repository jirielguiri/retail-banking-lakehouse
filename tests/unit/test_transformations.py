"""Unit tests for pure PySpark banking transformations."""

from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.utils.transformations import (
    add_credit_tier,
    add_delinquency_bucket,
    add_total_transaction_amount,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("retail-banking-unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_add_credit_tier_boundaries(spark):
    schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("credit_score", IntegerType(), True),
        ]
    )
    data = [
        ("C1", 765),   # Tier A (Jan Novak Day 2)
        ("C2", 750),   # Boundary A
        ("C3", 749),   # Boundary B upper
        ("C4", 680),   # Tier B (Jan Novak Day 1)
        ("C5", 650),   # Boundary B lower
        ("C6", 550),   # Boundary C lower
        ("C7", 549),   # Tier D
        ("C8", None),  # Null defaults to D
    ]
    df = spark.createDataFrame(data, schema)
    result = {
        row["customer_id"]: row["credit_tier"]
        for row in add_credit_tier(df).collect()
    }

    assert result == {
        "C1": "A",
        "C2": "A",
        "C3": "B",
        "C4": "B",
        "C5": "B",
        "C6": "C",
        "C7": "D",
        "C8": "D",
    }


def test_add_delinquency_bucket_ifrs9_thresholds(spark):
    schema = StructType(
        [
            StructField("contract_id", StringType(), True),
            StructField("days_past_due", IntegerType(), True),
        ]
    )
    data = [
        ("L1", 0),
        ("L2", None),
        ("L3", 1),
        ("L4", 30),
        ("L5", 31),
        ("L6", 89),
        ("L7", 90),
        ("L8", 120),
    ]
    df = spark.createDataFrame(data, schema)
    result = {
        row["contract_id"]: row["delinquency_bucket"]
        for row in add_delinquency_bucket(df).collect()
    }

    assert result == {
        "L1": "PERFORMING",
        "L2": "PERFORMING",
        "L3": "EARLY_ARREARS",
        "L4": "EARLY_ARREARS",
        "L5": "DELINQUENT",
        "L6": "DELINQUENT",
        "L7": "NPL_DEFAULT",
        "L8": "NPL_DEFAULT",
    }


def test_add_total_transaction_amount_handles_null_fee(spark):
    schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("principal_amount", DoubleType(), True),
            StructField("interest_amount", DoubleType(), True),
            StructField("fee_amount", DoubleType(), True),
        ]
    )
    data = [
        ("TXN-1", 7678.00, 18337.50, None),
        ("TXN-2", 3322.80, 1854.20, 500.00),
    ]
    df = spark.createDataFrame(data, schema)
    result = {
        row["transaction_id"]: row["total_transaction_amount"]
        for row in add_total_transaction_amount(df).collect()
    }

    assert result["TXN-1"] == Decimal("26015.50")
    assert result["TXN-2"] == Decimal("5677.00")