from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from utilities.abc import start_batch_run, start_job_run, end_job_run, end_batch_run
from utilities.dq import run_dq_checks

default_args = {
    "owner": "airflow",
}

with DAG(
    "silver_postgres_deployment",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    start_batch=PythonOperator(
        task_id="start_batch",
        python_callable=start_batch_run,
    )

    dbt_run_silver_airports = BashOperator(
        task_id='dbt_run_silver_airports',
        bash_command=(
            'cd /usr/app/dbt && '
            'dbt run --select silver.silver_airports'
        ),
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    dbt_test_silver_airports = PythonOperator(
        task_id='dbt_test_silver_airports',
        python_callable=run_dq_checks,
        op_kwargs={
            'table_schema': 'silver',
            'table_name': 'silver_airports'
        }
    )

    dbt_run_silver_airlines = BashOperator(
        task_id='dbt_run_silver_airlines',
        bash_command=(
            'cd /usr/app/dbt && '
            'dbt run --select silver.silver_airlines'
        ),
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    dbt_test_silver_airlines = PythonOperator(
        task_id='dbt_test_silver_airlines',
        python_callable=run_dq_checks,
        op_kwargs={
            'table_schema': 'silver',
            'table_name': 'silver_airlines'
        }
    )

    dbt_run_silver_cities = BashOperator(
        task_id='dbt_run_silver_cities',
        bash_command=(
            'cd /usr/app/dbt && '
            'dbt run --select silver.silver_cities'
        ),
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    dbt_test_silver_cities = PythonOperator(
        task_id='dbt_test_silver_cities',
        python_callable=run_dq_checks,
        op_kwargs={
            'table_schema': 'silver',
            'table_name': 'silver_cities'
        }
    )

    dbt_run_silver_flights = BashOperator(
        task_id='dbt_run_silver_flights',
        bash_command=(
            'cd /usr/app/dbt && '
            'dbt run --select silver.silver_flights'
        ),
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    dbt_test_silver_flights = PythonOperator(
        task_id='dbt_test_silver_flights',
        python_callable=run_dq_checks,
        op_kwargs={
            'table_schema': 'silver',
            'table_name': 'silver_flights'
        }
    )

    end_batch = PythonOperator(
        task_id="end_batch",
        python_callable=end_batch_run,
        trigger_rule="all_done"
    )

start_batch >> dbt_run_silver_airports >> dbt_test_silver_airports >> end_batch
start_batch >> dbt_run_silver_airlines >> dbt_test_silver_airlines >> end_batch
start_batch >> dbt_run_silver_cities >> dbt_test_silver_cities >> end_batch
start_batch >> dbt_run_silver_flights >> dbt_test_silver_flights >> end_batch
