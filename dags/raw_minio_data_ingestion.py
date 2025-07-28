from airflow import DAG
from datetime import datetime
from utilities.abc import start_batch_run, start_job_run, end_job_run, end_batch_run
from utilities.ingestion import ingest_data
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
}

with DAG(
    "raw_minio_data_ingestion",
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

    ingest_airlines_data = PythonOperator(
        task_id="ingest_airlines_data",
        python_callable=ingest_data,
        op_kwargs={"data_type": "airlines.json"},
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    ingest_airports_data = PythonOperator(
        task_id="ingest_airports_data",
        python_callable=ingest_data,
        op_kwargs={"data_type": "airports.json"},
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    ingest_cities_data = PythonOperator(
        task_id="ingest_cities_data",
        python_callable=ingest_data,
        op_kwargs={"data_type": "cities.json"},
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    ingest_opensky_data = PythonOperator(
        task_id="ingest_opensky_data",
        python_callable=ingest_data,
        op_kwargs={"data_type": "opensky.json"},
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    end_batch = PythonOperator(
        task_id="end_batch",
        python_callable=end_batch_run,
        trigger_rule="all_done"
    )

start_batch >> [ingest_airlines_data, ingest_airports_data, ingest_cities_data, ingest_opensky_data] >> end_batch
