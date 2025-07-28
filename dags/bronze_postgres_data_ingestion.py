from airflow import DAG
from datetime import datetime
from utilities.abc import start_batch_run, start_job_run, end_job_run, end_batch_run
from utilities.ingestion import ingest_data
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
}

# TBD - Need fresh logic to ingest json data from MinIO to Postgres

with DAG(
    "bronze_postgres_data_ingestion",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    tags=["minio", "ingestion"],
    catchup=False,
) as dag:
    start_batch=PythonOperator(
        task_id="start_batch",
        python_callable=start_batch_run,
    )

    end_batch = PythonOperator(
        task_id="end_batch",
        python_callable=end_batch_run,
        trigger_rule="all_done"
    )

start_batch >> end_batch