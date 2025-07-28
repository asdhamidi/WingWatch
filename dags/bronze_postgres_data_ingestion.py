from airflow import DAG
from datetime import datetime
from functools import partial
from utilities.abc import start_batch_run, start_job_run, end_job_run, end_batch_run
from utilities.postgres_ingestion import minio_to_postgres
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

    bronze_airline_ingestion = PythonOperator(
        task_id="bronze_airline_ingestion",
        python_callable=minio_to_postgres,
        op_kwargs={
            "bucket_name": "raw",
            "object_name": "airlines.json",
            "table_name": "bronze.bronze_airlines"
        },
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    bronze_airports_ingestion = PythonOperator(
        task_id="bronze_airports_ingestion",
        python_callable=minio_to_postgres,
        op_kwargs={
            "bucket_name": "raw",
            "object_name": "airports.json",
            "table_name": "bronze.bronze_airports"
        },
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    bronze_cities_ingestion = PythonOperator(
        task_id="bronze_cities_ingestion",
        python_callable=minio_to_postgres,
        op_kwargs={
            "bucket_name": "raw",
            "object_name": "cities.json",
            "table_name": "bronze.bronze_cities"
        },
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    bronze_flights_ingestion = PythonOperator(
        task_id="bronze_flights_ingestion",
        python_callable=minio_to_postgres,
        op_kwargs={
            "bucket_name": "raw",
            "object_name": "opensky.json",
            "table_name": "bronze.bronze_flights"
        },
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    end_batch = PythonOperator(
        task_id="end_batch",
        python_callable=end_batch_run,
        trigger_rule="all_done"
    )

start_batch >> [bronze_airline_ingestion, bronze_airports_ingestion, bronze_cities_ingestion, bronze_flights_ingestion] >> end_batch
