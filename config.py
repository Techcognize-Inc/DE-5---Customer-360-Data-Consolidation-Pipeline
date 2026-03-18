import os

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "bankingdb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

MASTER_DATA_PATH = os.getenv("MASTER_DATA_PATH", "./data/master/customers_master.csv")
DELTA_BASE_PATH = os.getenv("DELTA_BASE_PATH", "./data/delta")
CHECKPOINT_BASE_PATH = os.getenv("CHECKPOINT_BASE_PATH", "./data/checkpoints")

CUSTOMERS_TOPIC = "cdc.customers"
ACCOUNTS_TOPIC = "cdc.accounts"
LOANS_TOPIC = "cdc.loans"

STAGING_CUSTOMERS_PATH = f"{DELTA_BASE_PATH}/staging/customers"
STAGING_ACCOUNTS_PATH = f"{DELTA_BASE_PATH}/staging/accounts"
STAGING_LOANS_PATH = f"{DELTA_BASE_PATH}/staging/loans"

CUSTOMERS_CHECKPOINT = f"{CHECKPOINT_BASE_PATH}/customers"
ACCOUNTS_CHECKPOINT = f"{CHECKPOINT_BASE_PATH}/accounts"
LOANS_CHECKPOINT = f"{CHECKPOINT_BASE_PATH}/loans"

RESOLVED_CUSTOMERS_PATH = f"{DELTA_BASE_PATH}/resolved_customers"
CUSTOMER_360_PATH = f"{DELTA_BASE_PATH}/customer_360"
CUSTOMER_SEGMENTS_PATH = f"{DELTA_BASE_PATH}/customer_segments"

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}