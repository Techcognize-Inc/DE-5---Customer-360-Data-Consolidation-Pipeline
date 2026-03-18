import json
import os
import sys
import time
import uuid
from datetime import datetime
import pandas as pd
from kafka import KafkaProducer

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import CUSTOMERS_TOPIC, MASTER_DATA_PATH

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def build_event(payload, op_type):
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.utcnow().isoformat(),
        "op_type": op_type,
        "payload": json.dumps(payload)
    }

def main():
    df = pd.read_csv(MASTER_DATA_PATH)

    for i, row in df.iterrows():
        customer = row.to_dict()
        customer["updated_at"] = datetime.utcnow().isoformat()

        op_type = "INSERT"
        if i % 50 == 0:
            customer["city"] = f"{customer['city']}_UPDATED"
            op_type = "UPDATE"
        elif i % 200 == 0:
            op_type = "DELETE"

        producer.send(CUSTOMERS_TOPIC, value=build_event(customer, op_type))
        print(f"Sent {op_type} customer: {customer['customer_id']}")
        time.sleep(0.005)

    producer.flush()
    print("Customer producer completed.")

if __name__ == "__main__":
    main()