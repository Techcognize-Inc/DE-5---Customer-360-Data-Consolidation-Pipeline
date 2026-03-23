import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    countDistinct,
    current_timestamp,
    lit,
    rand as spark_rand,
    sum as spark_sum,
    when,
)

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import (
    SPARK_MASTER,
    RESOLVED_CUSTOMERS_PATH,
    CUSTOMER_360_PATH,
    WATERMARK_TABLE_PATH,
)
from watermark_utils import read_watermark, update_watermark

JOB_NAME = "customer360_join"
# Salt buckets for two-stage skew-resistant aggregation
N_SALTS = 8


def get_spark():
    spark = (
        SparkSession.builder
        .appName("DE5_Spark_Customer360_Join")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_customer_360_df(resolved_df):
    profile_dims = [
        "customer_id", "golden_customer_id",
        "first_name", "last_name", "email", "phone",
        "address", "city", "state", "country",
    ]

    profile_df = resolved_df.select(*profile_dims).dropDuplicates()

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

    salted_accounts = account_facts.withColumn("salt", (spark_rand() * N_SALTS).cast("int"))
    account_partial = salted_accounts.groupBy(profile_dims + ["salt"]).agg(
        spark_sum(
            when(col("balance").isNotNull(), col("balance")).otherwise(lit(0))
        ).alias("p_balance"),
        spark_sum(
            when(col("account_status") == "ACTIVE", lit(1)).otherwise(lit(0))
        ).alias("p_active_acct"),
    )
    account_metrics_df = account_partial.groupBy(profile_dims).agg(
        spark_sum("p_balance").alias("total_balance"),
        spark_sum("p_active_acct").alias("active_account_count"),
    )

    salted_loans = loan_facts.withColumn("salt", (spark_rand() * N_SALTS).cast("int"))
    loan_partial = salted_loans.groupBy(profile_dims + ["salt"]).agg(
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
    )
    loan_metrics_df = loan_partial.groupBy(profile_dims).agg(
        spark_sum("p_loan").alias("total_loan_amount"),
        spark_sum("p_active_loan").alias("active_loan_count"),
        spark_sum("p_delinquent").alias("delinquent_loan_count"),
        spark_sum("p_closed").alias("closed_loan_count"),
    )

    return (
        profile_df
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


def main():
    spark = get_spark()

    # ── 1. Read watermark ──────────────────────────────────────────────────────
    last_ts = read_watermark(spark, WATERMARK_TABLE_PATH, JOB_NAME)
    print(f"[{JOB_NAME}] Last processed timestamp: {last_ts}")

    # ── 2. Load resolved_customers, filter to rows resolved since watermark ────
    # resolved_at is stamped by spark_entity_resolution on every merge/insert.
    resolved_df = spark.read.format("delta").load(RESOLVED_CUSTOMERS_PATH)

    # Backward compatibility for older resolved tables created before
    # resolved_at / golden_customer_id were introduced.
    if "golden_customer_id" not in resolved_df.columns:
        resolved_df = resolved_df.withColumn("golden_customer_id", col("customer_id"))
    if "resolved_at" not in resolved_df.columns:
        resolved_df = resolved_df.withColumn("resolved_at", current_timestamp())

    new_resolved = resolved_df.filter(
        col("resolved_at") > lit(last_ts).cast("timestamp")
    )
    affected_ids = new_resolved.select("customer_id").distinct().cache()
    affected_count = affected_ids.count()

    if affected_count == 0:
        print(f"[{JOB_NAME}] No customers updated since {last_ts}. Skipping.")
        spark.stop()
        return

    print(f"[{JOB_NAME}] Aggregating {affected_count} updated customers.")
    affected_customer_ids = [row["customer_id"] for row in affected_ids.collect()]

    # ── 3. Aggregate ALL resolved rows for affected customers ──────────────────
    # Join back to full resolved_df so multi-account / multi-loan customers are
    # counted correctly — not just the incremental slice.
    affected_resolved = resolved_df.join(affected_ids, "customer_id", "inner")

    customer_360_df = build_customer_360_df(affected_resolved)

    # ── 4. Merge into customer_360 Delta table ─────────────────────────────────
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, CUSTOMER_360_PATH):
        escaped_ids = ", ".join(
            "'" + customer_id.replace("'", "''") + "'" for customer_id in affected_customer_ids
        )
        DeltaTable.forPath(spark, CUSTOMER_360_PATH).delete(
            f"customer_id IN ({escaped_ids})"
        )
        customer_360_df.write.format("delta").option("mergeSchema", "true").mode("append").save(CUSTOMER_360_PATH)
        print(f"[{JOB_NAME}] Replaced {affected_count} customers in {CUSTOMER_360_PATH}")
    else:
        customer_360_df.write.format("delta").mode("overwrite").save(CUSTOMER_360_PATH)
        print(f"[{JOB_NAME}] Initial write to {CUSTOMER_360_PATH}")

    # ── 5. Advance watermark ───────────────────────────────────────────────────
    new_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    update_watermark(spark, WATERMARK_TABLE_PATH, JOB_NAME, new_ts)
    print(f"[{JOB_NAME}] Watermark advanced to {new_ts}")

    print(f"[{JOB_NAME}] Output rows written: {customer_360_df.count()}")
    spark.stop()


if __name__ == "__main__":
    main()