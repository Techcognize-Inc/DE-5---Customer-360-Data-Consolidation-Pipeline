import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat, current_timestamp, lit, rand, row_number, udf, when,
)
from pyspark.sql.functions import max as spark_max, min as spark_min
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import (
    SPARK_MASTER,
    STAGING_CUSTOMERS_PATH,
    STAGING_ACCOUNTS_PATH,
    STAGING_LOANS_PATH,
    RESOLVED_CUSTOMERS_PATH,
    WATERMARK_TABLE_PATH,
)
from watermark_utils import read_watermark, update_watermark

JOB_NAME = "entity_resolution"
# Number of salt buckets for skew-resistant 3-way join
N_SALTS = 8


def get_spark():
    spark = (
        SparkSession.builder
        .appName("DE5_Entity_Resolution")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Enable Adaptive Query Execution for automatic skew-join optimisation
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
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


@udf(returnType=IntegerType())
def levenshtein_udf(s1: str, s2: str) -> int:
    """Levenshtein edit distance between two strings.

    Implemented as a Python UDF so the fuzzy-matching logic is transparent
    and testable without the native SQL levenshtein() requiring identical
    column types across all Spark versions.
    """
    if s1 is None or s2 is None:
        return 9999
    m, n = len(s1), len(s2)
    prev = list(range(n + 1))
    for i in range(1, m + 1):
        curr = [i] + [0] * n
        for j in range(1, n + 1):
            if s1[i - 1] == s2[j - 1]:
                curr[j] = prev[j - 1]
            else:
                curr[j] = 1 + min(prev[j], curr[j - 1], prev[j - 1])
        prev = curr
    return prev[n]


def assign_golden_customer_id(df):
    """Detect cross-system duplicate customers and assign a stable canonical ID.

    Two records are considered the same person when:
      - their email OR phone match exactly, AND
      - their full name Levenshtein distance <= 2  (handles typos / initials)

    golden_customer_id = lexicographically smallest customer_id in the match
    group, giving a reproducible canonical identifier per person.
    Customers with no fuzzy match keep their own customer_id as golden.
    """
    with_name = df.withColumn(
        "full_name", concat(col("first_name"), lit(" "), col("last_name"))
    )
    c1 = with_name.alias("c1")
    c2 = with_name.select("customer_id", "email", "phone", "full_name").alias("c2")

    # Only examine pairs where c1 < c2 to avoid duplicate pairs in the result
    pairs = (
        c1.join(
            c2,
            (col("c1.customer_id") < col("c2.customer_id"))
            & (
                (col("c1.email") == col("c2.email"))
                | (col("c1.phone") == col("c2.phone"))
            )
            & (levenshtein_udf(col("c1.full_name"), col("c2.full_name")) <= 2),
            "inner",
        )
        .select(
            col("c1.customer_id").alias("id_a"),
            col("c2.customer_id").alias("id_b"),
        )
    )

    # Build canonical mapping: golden = min(customer_id) across the match group.
    # Each pair (a, b) contributes both {a→a, a→b} and {b→a, b→b} so the
    # min() covers the customer itself AND its match partners.
    canonical = (
        pairs.selectExpr("id_a as customer_id", "id_a as candidate")
        .union(pairs.selectExpr("id_a as customer_id", "id_b as candidate"))
        .union(pairs.selectExpr("id_b as customer_id", "id_a as candidate"))
        .union(pairs.selectExpr("id_b as customer_id", "id_b as candidate"))
        .groupBy("customer_id")
        .agg(spark_min("candidate").alias("golden_customer_id"))
    )

    # Unmatched customers fall back to their own customer_id
    return (
        with_name
        .join(canonical, "customer_id", "left")
        .withColumn(
            "golden_customer_id",
            when(col("golden_customer_id").isNull(), col("customer_id"))
            .otherwise(col("golden_customer_id")),
        )
        .drop("full_name")
    )


def main():
    spark = get_spark()

    # ── 1. Load full staging tables ────────────────────────────────────────────
    customers_df = spark.read.format("delta").load(STAGING_CUSTOMERS_PATH)
    accounts_df  = spark.read.format("delta").load(STAGING_ACCOUNTS_PATH)
    loans_df     = spark.read.format("delta").load(STAGING_LOANS_PATH)

    # ── 2. Read watermark ──────────────────────────────────────────────────────
    last_ts = read_watermark(spark, WATERMARK_TABLE_PATH, JOB_NAME)
    print(f"[{JOB_NAME}] Last processed timestamp: {last_ts}")
    wm = lit(last_ts).cast("timestamp")

    # ── 3. Identify records newer than the watermark ───────────────────────────
    new_customers = customers_df.filter(col("event_time") > wm)
    new_accounts  = accounts_df.filter(col("event_time") > wm)
    new_loans     = loans_df.filter(col("event_time") > wm)

    # ── 4. Collect affected customer_ids across all three sources ──────────────
    affected_ids = (
        new_customers.select("customer_id")
        .union(new_accounts.select("customer_id"))
        .union(new_loans.select("customer_id"))
        .distinct()
        .cache()
    )
    affected_count = affected_ids.count()
    if affected_count == 0:
        print(f"[{JOB_NAME}] No new records since {last_ts}. Skipping.")
        spark.stop()
        return

    affected_customer_ids = [row["customer_id"] for row in affected_ids.collect()]

    print(f"[{JOB_NAME}] Re-resolving {affected_count} affected customers.")

    # ── 5. Get latest full records for affected customers only ─────────────────
    latest_customers_raw = get_latest_record(
        customers_df.join(affected_ids, "customer_id", "inner"), "customer_id"
    )
    latest_accounts = get_latest_record(
        accounts_df.join(affected_ids, "customer_id", "inner"), "account_id"
    )
    latest_loans = get_latest_record(
        loans_df.join(affected_ids, "customer_id", "inner"), "loan_id"
    )

    # ── 6. Fuzzy entity resolution: assign golden_customer_id ─────────────────
    # Detects cross-system duplicates via email/phone + Levenshtein name match.
    latest_customers = assign_golden_customer_id(latest_customers_raw)

    # ── 7. Salted 3-way join (skew-resistant) ──────────────────────────────────
    # Hot customer_ids (many accounts/loans) are distributed across N_SALTS
    # partitions by assigning each customer a random salt bucket, then
    # replicating the right-side tables with all salt values.
    salt_range = spark.range(N_SALTS).toDF("salt")
    c_salted = latest_customers.withColumn("salt", (rand() * N_SALTS).cast("int")).alias("c")
    a_salted = latest_accounts.crossJoin(salt_range).alias("a")
    l_salted = latest_loans.crossJoin(salt_range).alias("l")

    resolved_df = (
        c_salted
        .join(
            a_salted,
            (col("c.customer_id") == col("a.customer_id")) & (col("c.salt") == col("a.salt")),
            "left",
        )
        .join(
            l_salted,
            (col("c.customer_id") == col("l.customer_id")) & (col("c.salt") == col("l.salt")),
            "left",
        )
        .select(
            col("c.customer_id"),
            col("c.golden_customer_id"),
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
            col("l.loan_status"),
            current_timestamp().alias("resolved_at"),
        )
    )

    # ── 8. Merge into resolved_customers Delta table ───────────────────────────
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, RESOLVED_CUSTOMERS_PATH):
        escaped_ids = ", ".join(
            "'" + customer_id.replace("'", "''") + "'" for customer_id in affected_customer_ids
        )
        DeltaTable.forPath(spark, RESOLVED_CUSTOMERS_PATH).delete(
            f"customer_id IN ({escaped_ids})"
        )
        resolved_df.write.format("delta").option("mergeSchema", "true").mode("append").save(RESOLVED_CUSTOMERS_PATH)
        print(f"[{JOB_NAME}] Replaced {affected_count} customers in {RESOLVED_CUSTOMERS_PATH}")
    else:
        resolved_df.write.format("delta").mode("overwrite").save(RESOLVED_CUSTOMERS_PATH)
        print(f"[{JOB_NAME}] Initial write to {RESOLVED_CUSTOMERS_PATH}")

    # ── 9. Advance watermark to max event_time seen in this batch ──────────────
    new_max_ts_row = (
        new_customers.select(spark_max("event_time").alias("ts"))
        .union(new_accounts.select(spark_max("event_time").alias("ts")))
        .union(new_loans.select(spark_max("event_time").alias("ts")))
        .agg(spark_max("ts").alias("max_ts"))
        .collect()[0]
    )
    new_max_ts = str(new_max_ts_row["max_ts"]) if new_max_ts_row["max_ts"] else last_ts
    update_watermark(spark, WATERMARK_TABLE_PATH, JOB_NAME, new_max_ts)
    print(f"[{JOB_NAME}] Watermark advanced to {new_max_ts}")

    resolved_df.show(20, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()