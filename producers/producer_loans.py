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

from config import LOANS_TOPIC, MASTER_DATA_PATH

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

    for i in range(4000):
        customer = random.choice(customers)

        loan = {
            "loan_id": f"LOAN-{uuid.uuid4().hex[:10]}",
            "customer_id": customer["customer_id"],
            "loan_type": random.choice(["Home", "Auto", "Personal", "Education"]),
            "loan_amount": round(random.uniform(1000, 500000), 2),
            "loan_status": random.choice(["OPEN", "CLOSED", "DELINQUENT"]),
            "emi_amount": round(random.uniform(100, 8000), 2),
            "updated_at": datetime.utcnow().isoformat()
        }

        op_type = "INSERT"
        if i % 80 == 0:
            loan["loan_status"] = "DELINQUENT"
            op_type = "UPDATE"
        elif i % 350 == 0:
            op_type = "DELETE"

        producer.send(LOANS_TOPIC, value=build_event(loan, op_type))
        print(f"Sent {op_type} loan: {loan['loan_id']}")
        time.sleep(0.002)

    producer.flush()
    print("Loan producer completed.")

if __name__ == "__main__":
    main()