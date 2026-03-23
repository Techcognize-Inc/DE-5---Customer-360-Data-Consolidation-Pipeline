"""Great Expectations–style staging validation using PySpark.

Validates the three CDC staging Delta tables before they enter the
entity-resolution pipeline.  Checks:
  1. All required columns are present
  2. At least one row exists (table is not empty)
  3. Key identifier columns contain no nulls

Exits with a non-zero status code on failure so Airflow marks the
task as failed and stops the downstream pipeline.
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnull

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import (
    SPARK_MASTER,
    STAGING_CUSTOMERS_PATH,
    STAGING_ACCOUNTS_PATH,
    STAGING_LOANS_PATH,
)

# ── Expectation catalogue ──────────────────────────────────────────────────────
EXPECTATIONS = {
    "customers": {
        "path": STAGING_CUSTOMERS_PATH,
        "required_cols": [
            "event_id", "event_time", "op_type",
            "customer_id", "first_name", "last_name", "email", "phone",
        ],
        "not_null_cols": ["customer_id", "event_time", "op_type"],
        "min_rows": 1,
    },
    "accounts": {
        "path": STAGING_ACCOUNTS_PATH,
        "required_cols": [
            "event_id", "event_time", "op_type",
            "account_id", "customer_id", "account_type", "balance", "status",
        ],
        "not_null_cols": ["account_id", "customer_id", "event_time", "op_type"],
        "min_rows": 1,
    },
    "loans": {
        "path": STAGING_LOANS_PATH,
        "required_cols": [
            "event_id", "event_time", "op_type",
            "loan_id", "customer_id", "loan_type", "loan_amount",
        ],
        "not_null_cols": ["loan_id", "customer_id", "event_time", "op_type"],
        "min_rows": 1,
    },
}


def validate_table(spark, name, cfg):
    """Run all expectations for one staging table; return list of failure messages."""
    failures = []

    df = spark.read.format("delta").load(cfg["path"])
    actual_cols = set(df.columns)

    # 1. Schema: all required columns must be present
    for c in cfg["required_cols"]:
        if c not in actual_cols:
            failures.append(f"[{name}] Missing required column: '{c}'")

    # 2. Row count must meet the minimum threshold
    row_count = df.count()
    if row_count < cfg["min_rows"]:
        failures.append(
            f"[{name}] Row count {row_count} is below minimum {cfg['min_rows']}"
        )

    # 3. Null checks on key columns (only for columns that actually exist)
    checkable = [c for c in cfg["not_null_cols"] if c in actual_cols]
    if checkable:
        null_counts = (
            df.select(
                [count(when(isnull(col(c)), 1)).alias(c) for c in checkable]
            )
            .collect()[0]
            .asDict()
        )
        for col_name, null_n in null_counts.items():
            if null_n > 0:
                failures.append(
                    f"[{name}] Column '{col_name}' contains {null_n} null value(s)"
                )

    status = "PASS" if not failures else "FAIL"
    print(f"[GE] {name}: {row_count} rows — {status}")
    return failures


def main():
    spark = (
        SparkSession.builder
        .appName("DE5_GE_Staging_Validate")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    all_failures = []
    for name, cfg in EXPECTATIONS.items():
        all_failures.extend(validate_table(spark, name, cfg))

    spark.stop()

    if all_failures:
        print("\n[GE] Staging validation FAILED:")
        for msg in all_failures:
            print(f"  - {msg}")
        raise SystemExit(
            f"Staging validation failed with {len(all_failures)} error(s). "
            "Fix the data issues before re-running the pipeline."
        )

    print("[GE] All staging validation checks passed.")


if __name__ == "__main__":
    main()
