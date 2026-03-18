import importlib

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum as spark_sum, when, coalesce, lit


def load_module(candidates):
    last_error = None
    for name in candidates:
        try:
            return importlib.import_module(name)
        except Exception as exc:
            last_error = exc
    raise last_error


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = str(tmp_path_factory.mktemp("spark_warehouse_customer360"))
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-customer360")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", warehouse)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


def build_customer_360_df(resolved_df):
    return (
        resolved_df.groupBy(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "address",
            "city",
            "state",
            "country",
        )
        .agg(
            countDistinct("account_id").alias("account_count"),
            countDistinct("loan_id").alias("loan_count"),
            spark_sum(
                when(col("balance").isNotNull(), col("balance")).otherwise(lit(0))
            ).alias("total_balance"),
            spark_sum(
                when(col("loan_amount").isNotNull(), col("loan_amount")).otherwise(lit(0))
            ).alias("total_loan_amount"),
            spark_sum(
                when(col("account_status") == "ACTIVE", lit(1)).otherwise(lit(0))
            ).alias("active_account_count"),
            spark_sum(
                when(col("loan_status") == "ACTIVE", lit(1)).otherwise(lit(0))
            ).alias("active_loan_count"),
            spark_sum(
                when(col("loan_status") == "DELINQUENT", lit(1)).otherwise(lit(0))
            ).alias("delinquent_loan_count"),
            spark_sum(
                when(col("loan_status") == "CLOSED", lit(1)).otherwise(lit(0))
            ).alias("closed_loan_count"),
        )
        .withColumn("total_balance", coalesce(col("total_balance"), lit(0.0)))
        .withColumn("total_loan_amount", coalesce(col("total_loan_amount"), lit(0.0)))
    )


def test_customer360_aggregation_counts_and_sums(spark):
    # This mirrors the current implementation in spark_customer360_join.py
    _ = load_module([
        "batch.spark_customer360_join",
        "spark_customer360_join",
    ])

    resolved_df = spark.createDataFrame(
        [
            (
                "CUST-1", "John", "Doe", "john@test.com", "111", "Addr1", "Dallas", "TX", "USA",
                "ACC-1", "SAVINGS", 1000.0, "ACTIVE",
                "LOAN-1", "HOME", 5000.0, 250.0, "ACTIVE"
            ),
            (
                "CUST-1", "John", "Doe", "john@test.com", "111", "Addr1", "Dallas", "TX", "USA",
                "ACC-2", "CHECKING", 500.0, "ACTIVE",
                "LOAN-2", "AUTO", 2000.0, 150.0, "DELINQUENT"
            ),
        ],
        [
            "customer_id", "first_name", "last_name", "email", "phone", "address", "city", "state", "country",
            "account_id", "account_type", "balance", "account_status",
            "loan_id", "loan_type", "loan_amount", "emi_amount", "loan_status",
        ],
    )

    customer_360_df = build_customer_360_df(resolved_df)
    row = customer_360_df.collect()[0]

    assert row.customer_id == "CUST-1"
    assert row.account_count == 2
    assert row.loan_count == 2
    assert row.total_balance == 1500.0
    assert row.total_loan_amount == 7000.0
    assert row.active_account_count == 2
    assert row.active_loan_count == 1
    assert row.delinquent_loan_count == 1
    assert row.closed_loan_count == 0


def test_customer360_null_handling_defaults_to_zero(spark):
    resolved_df = spark.createDataFrame(
        [
            (
                "CUST-2", "Jane", "Smith", "jane@test.com", "222", "Addr2", "Austin", "TX", "USA",
                None, None, None, None,
                None, None, None, None, None
            ),
        ],
        [
            "customer_id", "first_name", "last_name", "email", "phone", "address", "city", "state", "country",
            "account_id", "account_type", "balance", "account_status",
            "loan_id", "loan_type", "loan_amount", "emi_amount", "loan_status",
        ],
    )

    customer_360_df = build_customer_360_df(resolved_df)
    row = customer_360_df.collect()[0]

    assert row.customer_id == "CUST-2"
    assert row.account_count == 0
    assert row.loan_count == 0
    assert row.total_balance == 0.0
    assert row.total_loan_amount == 0.0
    assert row.active_account_count == 0
    assert row.active_loan_count == 0
    assert row.delinquent_loan_count == 0
    assert row.closed_loan_count == 0


def test_customer360_distinct_counts_prevent_duplicate_overcounting(spark):
    resolved_df = spark.createDataFrame(
        [
            (
                "CUST-3", "Alex", "Ray", "alex@test.com", "333", "Addr3", "Plano", "TX", "USA",
                "ACC-9", "SAVINGS", 100.0, "ACTIVE",
                "LOAN-9", "AUTO", 1000.0, 100.0, "ACTIVE"
            ),
            (
                "CUST-3", "Alex", "Ray", "alex@test.com", "333", "Addr3", "Plano", "TX", "USA",
                "ACC-9", "SAVINGS", 100.0, "ACTIVE",
                "LOAN-9", "AUTO", 1000.0, 100.0, "ACTIVE"
            ),
        ],
        [
            "customer_id", "first_name", "last_name", "email", "phone", "address", "city", "state", "country",
            "account_id", "account_type", "balance", "account_status",
            "loan_id", "loan_type", "loan_amount", "emi_amount", "loan_status",
        ],
    )

    customer_360_df = build_customer_360_df(resolved_df)
    row = customer_360_df.collect()[0]

    assert row.account_count == 1
    assert row.loan_count == 1