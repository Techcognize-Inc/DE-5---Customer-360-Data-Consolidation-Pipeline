import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StringType

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import (
    SPARK_MASTER, KAFKA_BOOTSTRAP_SERVERS, CUSTOMERS_TOPIC,
    STAGING_CUSTOMERS_PATH, CUSTOMERS_CHECKPOINT, STREAM_WATERMARK_DELAY
)
from schemas import cdc_envelope_schema, customer_payload_schema

def get_spark():
    spark = (
        SparkSession.builder
        .appName("DE5_Consumer_Customers")
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
            from_json(col("cdc.payload").cast(StringType()), customer_payload_schema).alias("payload")
        )
        .select("event_id", "event_time", "op_type", "payload.*")
    )

def main():
    spark = get_spark()

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", CUSTOMERS_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = parse_stream(raw_df)
    watermarked_df = parsed_df.withWatermark("event_time", STREAM_WATERMARK_DELAY).dropDuplicates(["event_id"])

    query = (
        watermarked_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CUSTOMERS_CHECKPOINT)
        .start(STAGING_CUSTOMERS_PATH)
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()