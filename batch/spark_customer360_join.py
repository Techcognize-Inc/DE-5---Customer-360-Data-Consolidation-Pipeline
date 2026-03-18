import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    sum as spark_sum,
    when,
    coalesce,
    lit
)

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import RESOLVED_CUSTOMERS_PATH, CUSTOMER_360_PATH


def get_spark():
    spark = (
        SparkSession.builder
        .appName("DE5_Spark_Customer360_Join")
        .master("spark://spark-master:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = get_spark()

    resolved_df = spark.read.format("delta").load(RESOLVED_CUSTOMERS_PATH)

    customer_360_df = (
        resolved_df.groupBy(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "address",
            "city",
            "state",
            "country"
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
            ).alias("closed_loan_count")
        )
        .withColumn("total_balance", coalesce(col("total_balance"), lit(0.0)))
        .withColumn("total_loan_amount", coalesce(col("total_loan_amount"), lit(0.0)))
    )

    (
        customer_360_df.write
        .format("delta")
        .mode("overwrite")
        .save(CUSTOMER_360_PATH)
    )

    print(f"Customer 360 written to: {CUSTOMER_360_PATH}")
    customer_360_df.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()