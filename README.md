# FIFA SEER: End-to-End Data Pipeline for FIFA Player Data

## Overview
**FIFA SEER** is an end-to-end FIFA player data pipeline handling both batch and streaming data using PySpark, Kafka, Airflow, Hive, and dbt. Designed modular ETL scripts, orchestrated workflows, and exposed processed data via an API. Demonstrates full-stack data engineering and production-ready design.

**Key Highlights:**
- Ingests raw FIFA player data (CSV/JSON) and streaming events via **Kafka**.
- Processes data through **Bronze → Silver → Hive** layers.
- Uses **PySpark** for batch and streaming ETL transformations.
- Stores cleaned and transformed data in **Hive** for analytics.
- Models data with **dbt** (dimension and fact tables) to create a “gold” layer.
- Orchestrates the pipeline with **Airflow DAGs** (`fifa_pipeline`).
- **Flask API** to serve processed data.

## Architecture


## Airflow Completion:
<img width="2922" height="1838" alt="image" src="https://github.com/user-attachments/assets/83c6f91d-3ff9-4992-a6d8-d134f8a1a993" />
