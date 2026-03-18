import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StringType

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import (
    SPARK_MASTER, KAFKA_BOOTSTRAP_SERVERS, ACCOUNTS_TOPIC,
    STAGING_ACCOUNTS_PATH, ACCOUNTS_CHECKPOINT
)
from schemas import cdc_envelope_schema, account_payload_schema

def get_spark():
    spark = (
        SparkSession.builder
        .appName("DE5_Consumer_Accounts")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def parse_stream(raw_df):
    return (
        raw_df.selectExpr("CAST(value AS STRING) AS raw_json")
        .withColumn("cdc", from_json(col("raw_json"), cdc_envelope_schema))
        .select(
            col("cdc.event_id").alias("event_id"),
            to_timestamp(col("cdc.event_time")).alias("event_time"),
            col("cdc.op_type").alias("op_type"),
            from_json(col("cdc.payload").cast(StringType()), account_payload_schema).alias("payload")
        )
        .select("event_id", "event_time", "op_type", "payload.*")
    )

def main():
    spark = get_spark()

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", ACCOUNTS_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = parse_stream(raw_df)

    query = (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", ACCOUNTS_CHECKPOINT)
        .start(STAGING_ACCOUNTS_PATH)
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()