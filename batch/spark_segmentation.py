import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import DoubleType

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import CUSTOMER_360_PATH, CUSTOMER_SEGMENTS_PATH

def get_spark():
    spark = (
        SparkSession.builder
        .appName("DE5_Spark_Segmentation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    spark = get_spark()

    df = spark.read.format("delta").load(CUSTOMER_360_PATH)

    segmented_df = (
        df.select(
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
                "Platinum"
            ).when(
                (col("total_balance") >= 75000) | (col("total_loan_amount") >= 200000),
                "Gold"
            ).when(
                (col("total_balance") >= 25000) | (col("total_loan_amount") >= 50000),
                "Silver"
            ).otherwise("Dormant")
        )
    )

    segmented_df.write.format("delta").mode("overwrite").save(CUSTOMER_SEGMENTS_PATH)

    print(f"Customer Segments written to: {CUSTOMER_SEGMENTS_PATH}")
    segmented_df.show(20, truncate=False)

if __name__ == "__main__":
    main()