import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import (
    SPARK_MASTER,
    STAGING_CUSTOMERS_PATH,
    STAGING_ACCOUNTS_PATH,
    STAGING_LOANS_PATH,
    RESOLVED_CUSTOMERS_PATH
)


def get_spark():
    spark = (
        SparkSession.builder
        .appName("DE5_Entity_Resolution")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_latest_record(df, partition_col):
    window_spec = Window.partitionBy(partition_col).orderBy(col("event_time").desc())
    return (
        df.withColumn("rn", row_number().over(window_spec))
          .filter(col("rn") == 1)
          .drop("rn")
    )


def main():
    spark = get_spark()

    customers_df = spark.read.format("delta").load(STAGING_CUSTOMERS_PATH)
    accounts_df = spark.read.format("delta").load(STAGING_ACCOUNTS_PATH)
    loans_df = spark.read.format("delta").load(STAGING_LOANS_PATH)

    latest_customers = get_latest_record(customers_df, "customer_id").alias("c")
    latest_accounts = get_latest_record(accounts_df, "account_id").alias("a")
    latest_loans = get_latest_record(loans_df, "loan_id").alias("l")

    resolved_df = (
        latest_customers
        .join(latest_accounts, col("c.customer_id") == col("a.customer_id"), "left")
        .join(latest_loans, col("c.customer_id") == col("l.customer_id"), "left")
        .select(
            col("c.customer_id"),
            col("c.first_name"),
            col("c.last_name"),
            col("c.email"),
            col("c.phone"),
            col("c.address"),
            col("c.city"),
            col("c.state"),
            col("c.country"),
            col("a.account_id"),
            col("a.account_type"),
            col("a.balance"),
            col("a.status").alias("account_status"),
            col("l.loan_id"),
            col("l.loan_type"),
            col("l.loan_amount"),
            col("l.emi_amount"),
            col("l.loan_status")
        )
    )

    resolved_df.write.format("delta").mode("overwrite").save(RESOLVED_CUSTOMERS_PATH)

    print(f"Resolved customers written to: {RESOLVED_CUSTOMERS_PATH}")
    resolved_df.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()