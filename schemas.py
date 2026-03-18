from pyspark.sql.types import StructType, StructField, StringType, DoubleType

cdc_envelope_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("op_type", StringType(), True),
    StructField("payload", StringType(), True)
])

customer_payload_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("updated_at", StringType(), True)
])

account_payload_schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("updated_at", StringType(), True)
])

loan_payload_schema = StructType([
    StructField("loan_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("loan_type", StringType(), True),
    StructField("loan_amount", DoubleType(), True),
    StructField("loan_status", StringType(), True),
    StructField("emi_amount", DoubleType(), True),
    StructField("updated_at", StringType(), True)
])