from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from utilities.abc import start_batch_run, start_job_run, end_job_run, end_batch_run

default_args = {
    "owner": "airflow",
}

with DAG(
    "gold_biz_intel",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    tags=["aviation", "gold", "biz_intel"],
    catchup=False,
) as dag:
    start_batch=PythonOperator(
        task_id="start_batch",
        python_callable=start_batch_run,
    )

    dbt_run_gold_flights_by_airline = BashOperator(
        task_id='dbt_run_gold_flights_by_airline',
        bash_command=(
            'cd /usr/app/dbt && '
            'dbt run --select gold.gold_flights_by_airline'
        ),
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    dbt_run_gold_rare_aircrafts = BashOperator(
        task_id='dbt_run_gold_rare_aircrafts',
        bash_command=(
            'cd /usr/app/dbt && '
            'dbt run --select gold.gold_rare_aircrafts'
        ),
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    dbt_run_gold_airport_arrival_rate = BashOperator(
        task_id='dbt_run_gold_airport_arrival_rate',
        bash_command=(
            'cd /usr/app/dbt && '
            'dbt run --select gold.gold_airport_arrival_rate'
        ),
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    dbt_run_gold_country_traffic = BashOperator(
        task_id='dbt_run_gold_country_traffic',
        bash_command=(
            'cd /usr/app/dbt && '
            'dbt run --select gold.gold_country_traffic'
        ),
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    end_batch = PythonOperator(
        task_id="end_batch",
        python_callable=end_batch_run,
        trigger_rule="all_done"
    )

start_batch >> [dbt_run_gold_airport_arrival_rate, dbt_run_gold_flights_by_airline, dbt_run_gold_rare_aircrafts, dbt_run_gold_country_traffic] >> end_batch
