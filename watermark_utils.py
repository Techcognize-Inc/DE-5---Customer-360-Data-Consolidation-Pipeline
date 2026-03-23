"""
Shared helpers for Delta-backed watermark tracking in batch jobs.

Each job stores one row  (job_name, last_processed_timestamp)  in a
Delta table at WATERMARK_TABLE_PATH.  On the first ever run the table
does not yet exist, so read_watermark returns epoch-start and
update_watermark creates the table via an initial overwrite.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, StringType

EPOCH_START = "1970-01-01 00:00:00"

_SCHEMA = StructType([
    StructField("job_name", StringType(), False),
    StructField("last_processed_timestamp", StringType(), False),
])


def read_watermark(spark: SparkSession, path: str, job_name: str) -> str:
    """
    Return the last_processed_timestamp string recorded for job_name.
    Returns EPOCH_START when the table does not exist yet or the job
    has never run.
    """
    try:
        rows = (
            spark.read.format("delta").load(path)
            .filter(col("job_name") == job_name)
            .collect()
        )
        return str(rows[0]["last_processed_timestamp"]) if rows else EPOCH_START
    except Exception:
        return EPOCH_START


def update_watermark(
    spark: SparkSession, path: str, job_name: str, new_timestamp: str
) -> None:
    """
    Upsert (job_name, new_timestamp) into the Delta watermark table.
    Creates the table on first call.
    """
    from delta.tables import DeltaTable

    new_row = spark.createDataFrame([(job_name, new_timestamp)], _SCHEMA)

    if DeltaTable.isDeltaTable(spark, path):
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(new_row.alias("s"), "t.job_name = s.job_name")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        new_row.write.format("delta").mode("overwrite").save(path)
