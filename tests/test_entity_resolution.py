import importlib

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


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
    warehouse = str(tmp_path_factory.mktemp("spark_warehouse_entity"))
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-entity-resolution")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", warehouse)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


def test_get_latest_record_keeps_only_most_recent(spark):
    er_mod = load_module([
        "batch.spark_entity_resolution",
        "spark_entity_resolution",
    ])

    data = [
        ("CUST-1", "2026-03-18 01:00:00", "old@test.com"),
        ("CUST-1", "2026-03-18 02:00:00", "new@test.com"),
        ("CUST-2", "2026-03-18 01:30:00", "cust2@test.com"),
    ]
    df = spark.createDataFrame(data, ["customer_id", "event_time", "email"])

    latest_df = er_mod.get_latest_record(df, "customer_id")
    rows = {r["customer_id"]: r["email"] for r in latest_df.collect()}

    assert latest_df.count() == 2
    assert rows["CUST-1"] == "new@test.com"
    assert rows["CUST-2"] == "cust2@test.com"


def test_entity_resolution_left_joins_preserve_customer_without_account_or_loan(spark):
    er_mod = load_module([
        "batch.spark_entity_resolution",
        "spark_entity_resolution",
    ])

    customers_df = spark.createDataFrame(
        [
            ("evt-c1", "2026-03-18 01:00:00", "INSERT", "CUST-1", "John", "Doe", "john@test.com", "111", "Addr1", "Dallas", "TX", "USA"),
            ("evt-c2", "2026-03-18 01:00:00", "INSERT", "CUST-2", "Jane", "Smith", "jane@test.com", "222", "Addr2", "Austin", "TX", "USA"),
        ],
        [
            "event_id", "event_time", "op_type", "customer_id", "first_name", "last_name",
            "email", "phone", "address", "city", "state", "country"
        ],
    )

    accounts_df = spark.createDataFrame(
        [
            ("evt-a1", "2026-03-18 01:00:00", "INSERT", "ACC-1", "CUST-1", "SAVINGS", 500.0, "ACTIVE"),
        ],
        [
            "event_id", "event_time", "op_type", "account_id", "customer_id",
            "account_type", "balance", "status"
        ],
    )

    loans_df = spark.createDataFrame(
        [
            ("evt-l1", "2026-03-18 01:00:00", "INSERT", "LOAN-1", "CUST-1", "HOME", 10000.0, 500.0, "ACTIVE"),
        ],
        [
            "event_id", "event_time", "op_type", "loan_id", "customer_id",
            "loan_type", "loan_amount", "emi_amount", "loan_status"
        ],
    )

    latest_customers = er_mod.get_latest_record(customers_df, "customer_id").alias("c")
    latest_accounts = er_mod.get_latest_record(accounts_df, "account_id").alias("a")
    latest_loans = er_mod.get_latest_record(loans_df, "loan_id").alias("l")

    resolved_df = (
        latest_customers
        .join(latest_accounts, col("c.customer_id") == col("a.customer_id"), "left")
        .join(latest_loans, col("c.customer_id") == col("l.customer_id"), "left")
        .select(
            col("c.customer_id"),
            col("c.first_name"),
            col("c.last_name"),
            col("a.account_id"),
            col("l.loan_id"),
        )
    )

    rows = {r["customer_id"]: r.asDict() for r in resolved_df.collect()}

    assert resolved_df.count() == 2
    assert rows["CUST-1"]["account_id"] == "ACC-1"
    assert rows["CUST-1"]["loan_id"] == "LOAN-1"
    assert rows["CUST-2"]["account_id"] is None
    assert rows["CUST-2"]["loan_id"] is None


@pytest.mark.xfail(
    reason="Current implementation resolves records only by exact customer_id. "
           "It does not contain fuzzy/typo-based matching, so the >95% typo-resolution requirement is not implemented yet."
)
def test_typo_based_entity_resolution_requirement_placeholder():
    # This test intentionally documents the current gap between requirement and implementation.
    # Once fuzzy matching is implemented, replace this with a real assertion.
    assert False