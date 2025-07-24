# TuneStream ETL Pipeline with Apache Airflow + PostgreSQL

---

## Setup Steps

1. Created an `airflow/` directory with the following subfolders:
airflow/
├── dags/
│   ├── etl_pipeline.py
│   └── sql_queries.py
├── data/
│   ├── log_data/
│   └── song_data/
├── config/
│   └── create_tables.sql
├── docker-compose.yaml
├── README.md
2. Installed **Docker Desktop** for Windows. Required for container orchestration.

3. Downloaded the official `docker-compose.yaml` from the latest [Airflow releases] (I make some adjustment to install the psycopg2-binary)

4. Ran initial Airflow setup in terminal:
docker-compose up airflow-init
5. Run docker-compose up to start.

## DAG Workflow Overview
The DAG, tune_stream_dag, runs hourly and performs:

1. Staging of JSON logs and song metadata

2. Transforming and loading into these tables:

    - Fact: songplays

    - Dimensions: users, songs, artists, time

3. Data quality checks: verifying row counts, nulls, and uniqueness

## Data Quality Checks
After loading, the pipeline checks:

- Each table has at least one row

- Primary keys are not null

- Values are unique per primary key

Failures are logged and halt the DAG automatically


