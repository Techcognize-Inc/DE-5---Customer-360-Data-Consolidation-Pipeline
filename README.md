# DE-5 Customer 360 Data Consolidation Pipeline

## Flow
Faker source systems
-> Kafka topics (cdc.customers, cdc.accounts, cdc.loans)
-> PySpark Structured Streaming consumers
-> Delta staging tables
-> PySpark entity resolution
-> PySpark customer360 consolidation
-> PostgreSQL customer_360
-> PySpark segmentation
-> PostgreSQL customer_segments

## Run order

1. Start Docker
docker-compose up -d

2. Install packages
pip install -r requirements.txt

3. Generate Faker master data
python generate_master_data.py

4. Start consumers in 3 terminals
python consumers/consumer_customers.py
python consumers/consumer_accounts.py
python consumers/consumer_loans.py

5. Run producers in 3 terminals
python producers/producer_customers.py
python producers/producer_accounts.py
python producers/producer_loans.py

6. Run entity resolution
python batch/spark_entity_resolution.py

7. Run customer360 join
python batch/spark_customer360_join.py

8. Run segmentation
python batch/spark_segmentation.py
