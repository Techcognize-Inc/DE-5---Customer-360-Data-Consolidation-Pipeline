import os
import uuid
import pandas as pd

try:
    from faker import Faker
except ModuleNotFoundError as e:
    raise ModuleNotFoundError(
        "faker is required to run generate_master_data.py. "
        "Install dependencies with `pip install -r requirements.txt` "
        "or ensure the Airflow container mounts the project and installs requirements." 
    ) from e

from config import MASTER_DATA_PATH

fake = Faker()

def main():
    os.makedirs(os.path.dirname(MASTER_DATA_PATH), exist_ok=True)

    data = []
    for _ in range(5000):
        data.append({
            "customer_id": f"CUST-{uuid.uuid4().hex[:10]}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.msisdn()[:10],
            "address": fake.street_address(),
            "city": fake.city(),
            "state": fake.state(),
            "country": "USA"
        })

    df = pd.DataFrame(data)
    df.to_csv(MASTER_DATA_PATH, index=False)

    print(f"Generated Faker master customer file at: {MASTER_DATA_PATH}")
    print(df.head(10))

if __name__ == "__main__":
    main()