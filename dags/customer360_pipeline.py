from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "yourname",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

SPARK_CONF = (
    "--packages io.delta:delta-spark_2.12:3.2.0,io.openlineage:openlineage-spark_2.12:1.30.1 "
    "--conf spark.jars.ivy=/tmp/.ivy2 "
    "--conf spark.log.level=WARN "
    "--conf spark.ui.showConsoleProgress=false "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
    "--conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener "
    "--conf spark.openlineage.transport.type=console "
    "--conf spark.openlineage.namespace=customer360"
)

with DAG(
    dag_id="customer360_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 3, 17),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
) as dag:

    cdc_ingest_all_topics = BashOperator(
        task_id="cdc_ingest_all_topics",
        bash_command=(
            "cd /opt/project && "
            "python generate_master_data.py && "
            "python producers/producer_customers.py && "
            "python producers/producer_accounts.py && "
            "python producers/producer_loans.py"
        ),
    )

    ge_staging_validate = BashOperator(
        task_id="ge_staging_validate",
        bash_command=(
            "docker exec de5-spark-master bash -lc "
            "\"mkdir -p /tmp/.ivy2 && "
            "/opt/spark/bin/spark-submit "
            f"{SPARK_CONF} "
            "/opt/project/batch/ge_staging_validate.py\""
        ),
    )

    spark_entity_resolution = BashOperator(
        task_id="spark_entity_resolution",
        bash_command=(
            "docker exec de5-spark-master bash -lc "
            "\"mkdir -p /tmp/.ivy2 && "
            "/opt/spark/bin/spark-submit "
            f"{SPARK_CONF} "
            "/opt/project/batch/spark_entity_resolution.py\""
        ),
    )

    spark_customer360_join = BashOperator(
        task_id="spark_customer360_join",
        bash_command=(
            "docker exec de5-spark-master bash -lc "
            "\"mkdir -p /tmp/.ivy2 && "
            "/opt/spark/bin/spark-submit "
            f"{SPARK_CONF} "
            "/opt/project/batch/spark_customer360_join.py\""
        ),
    )

    spark_segmentation = BashOperator(
        task_id="spark_segmentation",
        bash_command=(
            "docker exec de5-spark-master bash -lc "
            "\"mkdir -p /tmp/.ivy2 && "
            "/opt/spark/bin/spark-submit "
            f"{SPARK_CONF} "
            "/opt/project/batch/spark_segmentation.py\""
        ),
    )

    openlineage_publish = BashOperator(
        task_id="openlineage_publish",
        bash_command="cd /opt/project && echo 'OpenLineage publish done'",
    )

    (
        cdc_ingest_all_topics
        >> ge_staging_validate
        >> spark_entity_resolution
        >> spark_customer360_join
        >> spark_segmentation
        >> openlineage_publish
    )