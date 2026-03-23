import importlib

import pytest
from pyspark.sql.functions import col


def load_module(candidates):
    last_error = None
    for name in candidates:
        try:
            return importlib.import_module(name)
        except Exception as exc:
            last_error = exc
    raise last_error


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


def test_fuzzy_entity_resolution_assigns_shared_golden_id(spark):
    """Customers with the same phone/email and near-identical names (edit
    distance <= 2) should receive the same golden_customer_id.
    Validates the levenshtein_udf + assign_golden_customer_id logic.
    """
    er_mod = load_module([
        "batch.spark_entity_resolution",
        "spark_entity_resolution",
    ])

    # CUST-A / CUST-B: same email & phone, "Jon Doe" vs "John Doe" (distance=1)
    # CUST-C: entirely different contact info — must keep its own golden id
    data = [
        ("CUST-A", "2026-03-18 01:00:00", "Jon",  "Doe",   "jon.doe@test.com", "1112223333"),
        ("CUST-B", "2026-03-18 01:00:00", "John", "Doe",   "jon.doe@test.com", "1112223333"),
        ("CUST-C", "2026-03-18 01:00:00", "Jane", "Smith", "jane@other.com",   "9998887777"),
    ]
    df = spark.createDataFrame(
        data, ["customer_id", "event_time", "first_name", "last_name", "email", "phone"]
    )

    result = er_mod.assign_golden_customer_id(df)
    rows = {r["customer_id"]: r["golden_customer_id"] for r in result.collect()}

    assert rows["CUST-A"] == rows["CUST-B"], (
        f"Expected CUST-A and CUST-B to share a golden_customer_id, got: {rows}"
    )
    assert rows["CUST-C"] == "CUST-C", (
        f"Expected CUST-C to keep its own golden_customer_id, got: {rows['CUST-C']}"
    )


def test_levenshtein_udf_computes_correct_distance(spark):
    """Unit-test the levenshtein_udf directly."""
    er_mod = load_module([
        "batch.spark_entity_resolution",
        "spark_entity_resolution",
    ])
    from pyspark.sql.functions import lit

    udf_fn = er_mod.levenshtein_udf
    df = spark.createDataFrame([("Jon Doe", "John Doe"), ("Alice", "Alice"), ("abc", "xyz")],
                               ["s1", "s2"])
    result = df.withColumn("dist", udf_fn("s1", "s2")).collect()
    dist_map = {(r["s1"], r["s2"]): r["dist"] for r in result}

    assert dist_map[("Jon Doe", "John Doe")] == 1   # insert 'h'
    assert dist_map[("Alice", "Alice")] == 0         # identical
    assert dist_map[("abc", "xyz")] == 3             # 3 substitutions