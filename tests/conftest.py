import os
import subprocess
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


def _configure_local_spark_env() -> Path:
    """Pin Spark runtime to the workspace venv's PySpark distribution."""
    spark_home = Path(sys.prefix) / "Lib" / "site-packages" / "pyspark"
    os.environ["SPARK_HOME"] = str(spark_home)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    return spark_home


def _spark_preflight_or_skip(spark_home: Path) -> None:
    """Fail fast if local Spark launcher is broken instead of hanging tests."""
    spark_submit = spark_home / "bin" / "spark-submit.cmd"
    if not spark_submit.exists():
        pytest.skip(f"spark-submit not found at {spark_submit}")

    try:
        subprocess.run(
            [str(spark_submit), "--version"],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except Exception as exc:
        pytest.skip(f"Local Spark preflight failed: {exc}")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    spark_home = _configure_local_spark_env()
    _spark_preflight_or_skip(spark_home)

    warehouse = str(tmp_path_factory.mktemp("spark_warehouse_shared"))
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-customer360")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", warehouse)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
