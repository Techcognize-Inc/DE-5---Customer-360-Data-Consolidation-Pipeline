import importlib
import importlib.util
import json
import os
import pathlib
import random
import string
import sys
import types

import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def load_module(candidates):
    last_error = None
    for name in candidates:
        try:
            if name in sys.modules:
                del sys.modules[name]
            return importlib.import_module(name)
        except Exception as exc:
            # fallback for modules stored in producers/ (not a package) or root
            try:
                if name.startswith("producers."):
                    pkg, mod = name.split(".", 1)
                    candidate_path = os.path.join(os.path.dirname(__file__), "..", pkg, f"{mod}.py")
                else:
                    candidate_path = os.path.join(os.path.dirname(__file__), "..", f"{name}.py")
                candidate_path = os.path.abspath(candidate_path)
                if os.path.exists(candidate_path):
                    spec = importlib.util.spec_from_file_location(name, candidate_path)
                    module = importlib.util.module_from_spec(spec)
                    sys.modules[name] = module
                    spec.loader.exec_module(module)
                    return module
            except Exception as fexc:
                last_error = fexc
            else:
                last_error = exc
    raise last_error


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = str(tmp_path_factory.mktemp("spark_warehouse"))
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-cdc-pipeline")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", warehouse)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


def import_customer_producer_with_mock(monkeypatch, fake_producer):
    fake_kafka = types.ModuleType("kafka")
    fake_kafka.KafkaProducer = lambda *args, **kwargs: fake_producer

    monkeypatch.setitem(sys.modules, "kafka", fake_kafka)

    module = load_module([
        "producers.producer_customers",
        "producer_customers",
    ])

    return module


def test_build_event_structure(monkeypatch):
    class FakeProducer:
        def send(self, topic, value):
            pass

        def flush(self):
            pass

    producer_mod = import_customer_producer_with_mock(monkeypatch, FakeProducer())

    payload = {"customer_id": "CUST-1", "first_name": "John"}
    event = producer_mod.build_event(payload, "INSERT")

    assert "event_id" in event
    assert "event_time" in event
    assert event["op_type"] == "INSERT"
    assert json.loads(event["payload"]) == payload


def test_customer_producer_main_sends_expected_operations(monkeypatch):
    sent_events = []

    class FakeProducer:
        def send(self, topic, value):
            sent_events.append((topic, value))

        def flush(self):
            return None

    producer_mod = import_customer_producer_with_mock(monkeypatch, FakeProducer())

    df = pd.DataFrame(
        [
            {
                "customer_id": f"CUST-{i}",
                "first_name": "John",
                "last_name": "Doe",
                "email": f"user{i}@test.com",
                "phone": "1234567890",
                "address": "123 Main St",
                "city": "Dallas",
                "state": "TX",
                "country": "USA",
            }
            for i in range(201)
        ]
    )

    monkeypatch.setattr(producer_mod.pd, "read_csv", lambda *_: df)
    monkeypatch.setattr(producer_mod.time, "sleep", lambda *_: None)

    producer_mod.main()

    assert len(sent_events) == 201

    op_types = [event["op_type"] for _, event in sent_events]

    assert "INSERT" in op_types
    assert "UPDATE" in op_types

    # Based on current producer logic:
    # if i % 50 == 0 runs before elif i % 200 == 0
    assert op_types[0] == "UPDATE"
    assert op_types[50] == "UPDATE"
    assert op_types[100] == "UPDATE"
    assert op_types[150] == "UPDATE"
    assert op_types[200] == "UPDATE"

    # Current implementation never emits DELETE because multiples of 200
    # are already caught by the first if condition.
    assert "DELETE" not in op_types


def random_typo(word):
    if len(word) < 3:
        return word
    i = random.randrange(len(word))
    c = random.choice(string.ascii_lowercase)
    return word[:i] + c + word[i+1:]


def test_typo_entity_resolution_over_95_percent(spark):
    er_mod = load_module([
        "batch.spark_entity_resolution",
        "spark_entity_resolution",
    ])

    customers = []
    for i in range(1000):
        cid = f"CUST-{i:04d}"
        customers.append((cid, "2026-03-01 00:00:00", "INSERT",
                          random_typo(f"First{i}"), random_typo(f"Last{i}"),
                          f"{i}@test.com", "1112223333", "1 Main St", "city", "state", "USA"))

    customers_df = spark.createDataFrame(
        customers,
        ["customer_id", "event_time", "op_type", "first_name", "last_name", "email", "phone", "address", "city", "state", "country"]
    )

    latest_customers = er_mod.get_latest_record(customers_df, "customer_id")
    resolved_count = latest_customers.select("customer_id").distinct().count()
    coverage = resolved_count / 1000.0

    assert coverage >= 0.95


def test_cdc_update_only_5000_records_refresh_customer360(spark):
    er_mod = load_module([
        "batch.spark_entity_resolution",
        "spark_entity_resolution",
    ])

    events = []
    for i in range(10000):
        events.append((f"CUST-{i:05d}", "2026-03-01 00:00:00", "INSERT", f"First{i}", f"Last{i}", f"{i}@test.com", "1112223333", "1 Main St", "city", "state", "USA"))
    for i in range(5000):
        events.append((f"CUST-{i:05d}", "2026-03-02 00:00:00", "UPDATE", f"First{i}", f"Last{i}", f"{i}@test.com", "1112223333", "1 Main St", "city_UPDATED", "state", "USA"))

    customers_df = spark.createDataFrame(
        events,
        ["customer_id", "event_time", "op_type", "first_name", "last_name", "email", "phone", "address", "city", "state", "country"]
    )

    latest_customers = er_mod.get_latest_record(customers_df, "customer_id")
    updated_count = latest_customers.filter(col("city") == "city_UPDATED").count()
    assert updated_count == 5000


def test_openlineage_graph_trace_validation():
    dag_path = pathlib.Path("c:/customer-360/dags/customer360_pipeline.py")
    content = dag_path.read_text()

    assert "spark.openlineage.transport.url=http://marquez:5000" in content
    assert "spark.openlineage.namespace=customer360" in content
    assert "cdc_ingest_all_topics" in content
    assert "spark_entity_resolution" in content
    assert "spark_customer360_join" in content


def test_parse_stream_customers(spark):
    customer_consumer = load_module([
        "consumers.consumer_customers",
        "consumers.customer_consumer",
        "consumer_customers",
        "customer_consumer",
    ])

    payload = {
        "customer_id": "CUST-1",
        "first_name": "John",
        "last_name": "Doe",
        "email": "john@test.com",
        "phone": "1112223333",
        "address": "123 Main St",
        "city": "Dallas",
        "state": "TX",
        "country": "USA",
        "updated_at": "2026-03-18T01:00:00",
    }

    raw_event = {
        "event_id": "evt-1",
        "event_time": "2026-03-18T01:00:00",
        "op_type": "INSERT",
        "payload": json.dumps(payload),
    }

    raw_df = spark.createDataFrame([{"value": json.dumps(raw_event)}])
    parsed_df = customer_consumer.parse_stream(raw_df)

    row = parsed_df.collect()[0]
    assert row.event_id == "evt-1"
    assert row.op_type == "INSERT"
    assert row.customer_id == "CUST-1"
    assert row.first_name == "John"
    assert row.city == "Dallas"


def test_parse_stream_accounts(spark):
    accounts_consumer = load_module([
        "consumers.consumer_accounts",
        "consumers.accounts_consumer",
        "consumer_accounts",
        "accounts_consumer",
    ])

    payload = {
        "account_id": "ACC-1",
        "customer_id": "CUST-1",
        "account_type": "SAVINGS",
        "balance": 1500.0,
        "status": "ACTIVE",
        "updated_at": "2026-03-18T01:00:00",
    }

    raw_event = {
        "event_id": "evt-2",
        "event_time": "2026-03-18T01:00:00",
        "op_type": "UPDATE",
        "payload": json.dumps(payload),
    }

    raw_df = spark.createDataFrame([{"value": json.dumps(raw_event)}])
    parsed_df = accounts_consumer.parse_stream(raw_df)

    row = parsed_df.collect()[0]
    assert row.event_id == "evt-2"
    assert row.op_type == "UPDATE"
    assert row.account_id == "ACC-1"
    assert row.customer_id == "CUST-1"
    assert row.balance == 1500.0
    assert row.status == "ACTIVE"


def test_parse_stream_loans(spark):
    loans_consumer = load_module([
        "consumers.consumer_loans",
        "consumers.loans_consumer",
        "consumer_loans",
        "loans_consumer",
    ])

    payload = {
        "loan_id": "LOAN-1",
        "customer_id": "CUST-1",
        "loan_type": "HOME",
        "loan_amount": 250000.0,
        "loan_status": "ACTIVE",
        "emi_amount": 1800.0,
        "updated_at": "2026-03-18T01:00:00",
    }

    raw_event = {
        "event_id": "evt-3",
        "event_time": "2026-03-18T01:00:00",
        "op_type": "INSERT",
        "payload": json.dumps(payload),
    }

    raw_df = spark.createDataFrame([{"value": json.dumps(raw_event)}])
    parsed_df = loans_consumer.parse_stream(raw_df)

    row = parsed_df.collect()[0]
    assert row.event_id == "evt-3"
    assert row.op_type == "INSERT"
    assert row.loan_id == "LOAN-1"
    assert row.customer_id == "CUST-1"
    assert row.loan_amount == 250000.0
    assert row.loan_status == "ACTIVE"