import json
import os
import sys
import random
import time
import uuid
from datetime import datetime
import pandas as pd
from kafka import KafkaProducer

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import ACCOUNTS_TOPIC, MASTER_DATA_PATH

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
    customers = df.to_dict(orient="records")

    for i in range(7000):
        customer = random.choice(customers)

        account = {
            "account_id": f"ACC-{uuid.uuid4().hex[:10]}",
            "customer_id": customer["customer_id"],
            "account_type": random.choice(["Savings", "Checking", "Credit"]),
            "balance": round(random.uniform(100, 100000), 2),
            "status": random.choice(["ACTIVE", "INACTIVE", "BLOCKED"]),
            "updated_at": datetime.utcnow().isoformat()
        }

        op_type = "INSERT"
        if i % 70 == 0:
            account["balance"] = round(account["balance"] + 5000, 2)
            op_type = "UPDATE"
        elif i % 300 == 0:
            op_type = "DELETE"

        producer.send(ACCOUNTS_TOPIC, value=build_event(account, op_type))
        print(f"Sent {op_type} account: {account['account_id']}")
        time.sleep(0.002)

    producer.flush()
    print("Account producer completed.")

if __name__ == "__main__":
    main()