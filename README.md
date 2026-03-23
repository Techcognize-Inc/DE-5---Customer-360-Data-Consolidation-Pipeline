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

Optional streaming setting
Set STREAM_WATERMARK_DELAY to control how much late-arriving CDC data is accepted before Spark evicts old state. Default is 10 minutes.

5. Run producers in 3 terminals
python producers/producer_customers.py
python producers/producer_accounts.py
python producers/producer_loans.py

6. Validate staging data
python batch/ge_staging_validate.py

7. Run entity resolution
python batch/spark_entity_resolution.py

8. Run customer360 join
python batch/spark_customer360_join.py

9. Run segmentation
python batch/spark_segmentation.py

## Notes

- Batch jobs use a Delta watermark table to process only new data since the last successful run.
- Entity resolution now assigns `golden_customer_id` using email/phone exact matches plus Levenshtein name distance `<= 2`.
- Customer 360 metrics aggregate accounts and loans separately to avoid account-loan cartesian overcounting.

## Scope Completed

This project now includes the full customer 360 batch + streaming workflow with incremental processing and operational validation:

- Streaming CDC ingestion from Kafka to Delta staging for customers, accounts, and loans
- Event-time watermarking and dedup in streaming consumers
- Delta watermark-driven incremental batch processing
- Entity resolution with fuzzy matching and canonical `golden_customer_id`
- Skew-safe join/aggregation patterns using salting
- Customer 360 consolidation with overcount-safe account/loan aggregation
- Segmentation output generation
- Airflow DAG orchestration path
- Staging data validation task

## Key Technical Deliverables

1. Watermarking
- Streaming watermark in consumers (`STREAM_WATERMARK_DELAY`)
- Batch watermark table (`watermark_config` Delta path)
- Shared watermark helpers in `watermark_utils.py`

2. Entity Resolution
- Added Levenshtein UDF-based fuzzy matching
- Added `assign_golden_customer_id(...)`
- Added `golden_customer_id` output field
- Incremental replacement strategy for affected customer IDs
- Schema evolution support for legacy Delta tables

3. Customer 360 Consolidation
- Added backward compatibility for legacy schemas
- Switched to account-facts and loan-facts separated aggregation to avoid cartesian overcounting
- Added skew-safe salted aggregation pattern
- Added `updated_at` and safe replacement strategy for incremental refresh

4. Segmentation
- Incremental segmentation by `updated_at`
- Legacy compatibility fallback if `updated_at` is missing

5. Validation and Tests
- Added real staging validation script (`batch/ge_staging_validate.py`)
- Replaced placeholder fuzzy test with real assertions
- Expanded customer360 tests including overcounting guard case
- Added shared Spark test fixture in `tests/conftest.py` to stabilize local runs

## Operational Validation Status

- Containerized Spark batch path executed successfully for:
	- staging validation
	- entity resolution
	- customer360 join
	- segmentation
- Delta outputs confirmed in `data/delta/`:
	- `resolved_customers`
	- `customer_360`
	- `customer_segments`
	- `staging`
	- `watermark_config`

## Known Environment Note

Local Windows PySpark startup can be impacted by machine-level Spark/Python env mismatches.
The test fixture now forces workspace venv Spark/Python settings to avoid hanging gateway startup.

## Recommended Final Commands

```powershell
python batch/ge_staging_validate.py
python batch/spark_entity_resolution.py
python batch/spark_customer360_join.py
python batch/spark_segmentation.py
python -m pytest tests/test_entity_resolution.py tests/test_customer360.py -v
```

## Final Validation Checklist

Use this checklist before final submission:

- [ ] Docker stack is healthy (`docker compose up -d --force-recreate` and all required containers are Up)
- [ ] Staging validation passes (`python batch/ge_staging_validate.py` or equivalent Spark container run)
- [ ] Entity resolution passes (`python batch/spark_entity_resolution.py`)
- [ ] Customer 360 consolidation passes (`python batch/spark_customer360_join.py`)
- [ ] Segmentation passes (`python batch/spark_segmentation.py`)
- [ ] Delta outputs exist in `data/delta/`: `resolved_customers`, `customer_360`, `customer_segments`, `watermark_config`
- [ ] Watermark values are advancing between runs (no full reprocessing on unchanged data)
- [ ] Two consecutive orchestrated DAG runs succeed in Airflow (`customer360_pipeline`)
- [ ] Tests run and report pass/skip clearly:
	- `python -m pytest tests/test_entity_resolution.py tests/test_customer360.py -v`

## Release / Sign-Off Steps

1. Review changed files

```
git status
```

2. Stage final deliverables

```
git add config.py watermark_utils.py batch/ data/batch/ dags/customer360_pipeline.py tests/ README.md
```

3. Commit

```
git commit -m "feat: complete DE5 customer360 pipeline with incremental watermarking, fuzzy ER, skew-safe aggregation, and validation"
```

4. Tag milestone

```
git tag -a v1.0-signoff -m "Customer360 DE5 sign-off"
```

5. Optional: push branch and tag

```
git push
git push origin v1.0-signoff
```
