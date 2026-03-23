import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when
from pyspark.sql.types import DoubleType

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import (
    SPARK_MASTER,
    CUSTOMER_360_PATH,
    CUSTOMER_SEGMENTS_PATH,
    WATERMARK_TABLE_PATH,
)
from watermark_utils import read_watermark, update_watermark

JOB_NAME = "segmentation"


def get_spark():
    spark = (
        SparkSession.builder
        .appName("DE5_Spark_Segmentation")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = get_spark()

    # ── 1. Read watermark ──────────────────────────────────────────────────────
    last_ts = read_watermark(spark, WATERMARK_TABLE_PATH, JOB_NAME)
    print(f"[{JOB_NAME}] Last processed timestamp: {last_ts}")

    # ── 2. Load customer_360, filter to rows updated since watermark ───────────
    # updated_at is stamped by spark_customer360_join on every merge/insert.
    df = spark.read.format("delta").load(CUSTOMER_360_PATH)
    if "updated_at" not in df.columns:
        df = df.withColumn("updated_at", current_timestamp())
    new_df = df.filter(col("updated_at") > lit(last_ts).cast("timestamp"))
    affected_count = new_df.count()

    if affected_count == 0:
        print(f"[{JOB_NAME}] No customers updated since {last_ts}. Skipping.")
        spark.stop()
        return

    print(f"[{JOB_NAME}] Segmenting {affected_count} updated customers.")

    # ── 3. Apply segmentation logic to changed customers only ──────────────────
    segmented_df = (
        new_df.select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
            col("phone"),
            col("city"),
            col("state"),
            col("country"),
            col("account_count").cast(DoubleType()),
            col("loan_count").cast(DoubleType()),
            col("total_balance").cast(DoubleType()),
            col("total_loan_amount").cast(DoubleType()),
            col("active_account_count").cast(DoubleType()),
            col("active_loan_count").cast(DoubleType()),
            col("delinquent_loan_count").cast(DoubleType()),
            col("closed_loan_count").cast(DoubleType()),
        )
        .withColumn(
            "segment",
            when(
                (col("total_balance") >= 150000) | (col("total_loan_amount") >= 500000),
                "Platinum",
            ).when(
                (col("total_balance") >= 75000) | (col("total_loan_amount") >= 200000),
                "Gold",
            ).when(
                (col("total_balance") >= 25000) | (col("total_loan_amount") >= 50000),
                "Silver",
            ).otherwise("Dormant")
        )
    )

    # ── 4. Merge into customer_segments Delta table ────────────────────────────
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, CUSTOMER_SEGMENTS_PATH):
        (
            DeltaTable.forPath(spark, CUSTOMER_SEGMENTS_PATH)
            .alias("target")
            .merge(
                segmented_df.alias("source"),
                "target.customer_id = source.customer_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[{JOB_NAME}] Merged {affected_count} segments into {CUSTOMER_SEGMENTS_PATH}")
    else:
        segmented_df.write.format("delta").mode("overwrite").save(CUSTOMER_SEGMENTS_PATH)
        print(f"[{JOB_NAME}] Initial write to {CUSTOMER_SEGMENTS_PATH}")

    # ── 5. Advance watermark ───────────────────────────────────────────────────
    new_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    update_watermark(spark, WATERMARK_TABLE_PATH, JOB_NAME, new_ts)
    print(f"[{JOB_NAME}] Watermark advanced to {new_ts}")

    segmented_df.show(20, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()