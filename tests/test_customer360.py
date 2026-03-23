import importlib

from pyspark.sql.functions import col, countDistinct, current_timestamp, sum as spark_sum, when, coalesce, lit, rand as spark_rand


def load_module(candidates):
    last_error = None
    for name in candidates:
        try:
            return importlib.import_module(name)
        except Exception as exc:
            last_error = exc
    raise last_error


def build_customer_360_df(resolved_df):
    profile_dims = [
        "customer_id",
        "golden_customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "address",
        "city",
        "state",
        "country",
    ]

    account_facts = resolved_df.select(
        *(profile_dims + ["account_id", "balance", "account_status"])
    ).dropDuplicates(["customer_id", "account_id"])
    loan_facts = resolved_df.select(
        *(profile_dims + ["loan_id", "loan_amount", "loan_status"])
    ).dropDuplicates(["customer_id", "loan_id"])

    account_count_df = account_facts.groupBy(profile_dims).agg(
        countDistinct("account_id").alias("account_count")
    )
    loan_count_df = loan_facts.groupBy(profile_dims).agg(
        countDistinct("loan_id").alias("loan_count")
    )

    salted_accounts = account_facts.withColumn("salt", (spark_rand() * 8).cast("int"))
    account_metrics_df = salted_accounts.groupBy(profile_dims + ["salt"]).agg(
        spark_sum(
            when(col("balance").isNotNull(), col("balance")).otherwise(lit(0))
        ).alias("p_balance"),
        spark_sum(
            when(col("account_status") == "ACTIVE", lit(1)).otherwise(lit(0))
        ).alias("p_active_acct"),
    ).groupBy(profile_dims).agg(
        spark_sum("p_balance").alias("total_balance"),
        spark_sum("p_active_acct").alias("active_account_count"),
    )

    salted_loans = loan_facts.withColumn("salt", (spark_rand() * 8).cast("int"))
    loan_metrics_df = salted_loans.groupBy(profile_dims + ["salt"]).agg(
        spark_sum(
            when(col("loan_amount").isNotNull(), col("loan_amount")).otherwise(lit(0))
        ).alias("p_loan"),
        spark_sum(
            when(col("loan_status") == "ACTIVE", lit(1)).otherwise(lit(0))
        ).alias("p_active_loan"),
        spark_sum(
            when(col("loan_status") == "DELINQUENT", lit(1)).otherwise(lit(0))
        ).alias("p_delinquent"),
        spark_sum(
            when(col("loan_status") == "CLOSED", lit(1)).otherwise(lit(0))
        ).alias("p_closed"),
    ).groupBy(profile_dims).agg(
        spark_sum("p_loan").alias("total_loan_amount"),
        spark_sum("p_active_loan").alias("active_loan_count"),
        spark_sum("p_delinquent").alias("delinquent_loan_count"),
        spark_sum("p_closed").alias("closed_loan_count"),
    )

    return (
        resolved_df.select(*profile_dims).dropDuplicates()
        .join(account_count_df, profile_dims, "left")
        .join(loan_count_df, profile_dims, "left")
        .join(account_metrics_df, profile_dims, "left")
        .join(loan_metrics_df, profile_dims, "left")
        .withColumn("account_count", coalesce(col("account_count"), lit(0)))
        .withColumn("loan_count", coalesce(col("loan_count"), lit(0)))
        .withColumn("total_balance", coalesce(col("total_balance"), lit(0.0)))
        .withColumn("total_loan_amount", coalesce(col("total_loan_amount"), lit(0.0)))
        .withColumn("active_account_count", coalesce(col("active_account_count"), lit(0)))
        .withColumn("active_loan_count", coalesce(col("active_loan_count"), lit(0)))
        .withColumn("delinquent_loan_count", coalesce(col("delinquent_loan_count"), lit(0)))
        .withColumn("closed_loan_count", coalesce(col("closed_loan_count"), lit(0)))
        .withColumn("updated_at", current_timestamp())
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
                "CUST-1", "GOLD-1", "John", "Doe", "john@test.com", "111", "Addr1", "Dallas", "TX", "USA",
                "ACC-1", "SAVINGS", 1000.0, "ACTIVE",
                "LOAN-1", "HOME", 5000.0, 250.0, "ACTIVE"
            ),
            (
                "CUST-1", "GOLD-1", "John", "Doe", "john@test.com", "111", "Addr1", "Dallas", "TX", "USA",
                "ACC-2", "CHECKING", 500.0, "ACTIVE",
                "LOAN-2", "AUTO", 2000.0, 150.0, "DELINQUENT"
            ),
        ],
        [
            "customer_id", "golden_customer_id", "first_name", "last_name", "email", "phone", "address", "city", "state", "country",
            "account_id", "account_type", "balance", "account_status",
            "loan_id", "loan_type", "loan_amount", "emi_amount", "loan_status",
        ],
    )

    customer_360_df = build_customer_360_df(resolved_df)
    row = customer_360_df.collect()[0]

    assert row.customer_id == "CUST-1"
    assert row.golden_customer_id == "GOLD-1"
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
                "CUST-2", "CUST-2", "Jane", "Smith", "jane@test.com", "222", "Addr2", "Austin", "TX", "USA",
                None, None, None, None,
                None, None, None, None, None
            ),
        ],
        [
            "customer_id", "golden_customer_id", "first_name", "last_name", "email", "phone", "address", "city", "state", "country",
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
                "CUST-3", "CUST-3", "Alex", "Ray", "alex@test.com", "333", "Addr3", "Plano", "TX", "USA",
                "ACC-9", "SAVINGS", 100.0, "ACTIVE",
                "LOAN-9", "AUTO", 1000.0, 100.0, "ACTIVE"
            ),
            (
                "CUST-3", "CUST-3", "Alex", "Ray", "alex@test.com", "333", "Addr3", "Plano", "TX", "USA",
                "ACC-9", "SAVINGS", 100.0, "ACTIVE",
                "LOAN-9", "AUTO", 1000.0, 100.0, "ACTIVE"
            ),
        ],
        [
            "customer_id", "golden_customer_id", "first_name", "last_name", "email", "phone", "address", "city", "state", "country",
            "account_id", "account_type", "balance", "account_status",
            "loan_id", "loan_type", "loan_amount", "emi_amount", "loan_status",
        ],
    )

    customer_360_df = build_customer_360_df(resolved_df)
    row = customer_360_df.collect()[0]

    assert row.account_count == 1
    assert row.loan_count == 1


def test_customer360_avoids_cartesian_overcounting_for_multiple_accounts_and_loans(spark):
    resolved_df = spark.createDataFrame(
        [
            (
                "CUST-4", "CUST-4", "Mia", "West", "mia@test.com", "444", "Addr4", "Houston", "TX", "USA",
                "ACC-1", "SAVINGS", 1000.0, "ACTIVE",
                "LOAN-1", "HOME", 5000.0, 250.0, "ACTIVE"
            ),
            (
                "CUST-4", "CUST-4", "Mia", "West", "mia@test.com", "444", "Addr4", "Houston", "TX", "USA",
                "ACC-1", "SAVINGS", 1000.0, "ACTIVE",
                "LOAN-2", "AUTO", 2000.0, 100.0, "DELINQUENT"
            ),
            (
                "CUST-4", "CUST-4", "Mia", "West", "mia@test.com", "444", "Addr4", "Houston", "TX", "USA",
                "ACC-2", "CHECKING", 500.0, "ACTIVE",
                "LOAN-1", "HOME", 5000.0, 250.0, "ACTIVE"
            ),
            (
                "CUST-4", "CUST-4", "Mia", "West", "mia@test.com", "444", "Addr4", "Houston", "TX", "USA",
                "ACC-2", "CHECKING", 500.0, "ACTIVE",
                "LOAN-2", "AUTO", 2000.0, 100.0, "DELINQUENT"
            ),
        ],
        [
            "customer_id", "golden_customer_id", "first_name", "last_name", "email", "phone", "address", "city", "state", "country",
            "account_id", "account_type", "balance", "account_status",
            "loan_id", "loan_type", "loan_amount", "emi_amount", "loan_status",
        ],
    )

    row = build_customer_360_df(resolved_df).collect()[0]

    assert row.account_count == 2
    assert row.loan_count == 2
    assert row.total_balance == 1500.0
    assert row.total_loan_amount == 7000.0
    assert row.active_account_count == 2
    assert row.active_loan_count == 1
    assert row.delinquent_loan_count == 1