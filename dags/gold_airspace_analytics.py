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
    "gold_airspace_analytics",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    tags=["aviation", "gold", "airspace_analytics"],
    catchup=False,
    schedule="@hourly"
) as dag:
    start_batch=PythonOperator(
        task_id="start_batch",
        python_callable=start_batch_run,
    )

    dbt_run_gold_altitude_band = BashOperator(
        task_id='dbt_run_gold_altitude_band',
        bash_command=(
            'cd /usr/app/dbt && '
            'dbt run --select gold.gold_altitude_band'
        ),
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    dbt_test_gold_altitude_band = PythonOperator(
        task_id='dbt_test_gold_altitude_band',
        python_callable=run_dq_checks,
        op_kwargs={
            'table_schema': 'gold',
            'table_name': 'gold_altitude_band'
        }
    )

    dbt_run_gold_grid_analysis = BashOperator(
        task_id='dbt_run_gold_grid_analysis',
        bash_command=(
            'cd /usr/app/dbt && '
            'dbt run --select gold.gold_grid_analysis'
        ),
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    dbt_test_gold_grid_analysis = PythonOperator(
        task_id='dbt_test_gold_grid_analysis',
        python_callable=run_dq_checks,
        op_kwargs={
            'table_schema': 'gold',
            'table_name': 'gold_grid_analysis'
        }
    )

    dbt_run_gold_supersonic_flights = BashOperator(
        task_id='dbt_run_gold_supersonic_flights',
        bash_command=(
            'cd /usr/app/dbt && '
            'dbt run --select gold.gold_supersonic_flights'
        ),
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    dbt_test_gold_supersonic_flights = PythonOperator(
        task_id='dbt_test_gold_supersonic_flights',
        python_callable=run_dq_checks,
        op_kwargs={
            'table_schema': 'gold',
            'table_name': 'gold_supersonic_flights'
        }
    )

    end_batch = PythonOperator(
        task_id="end_batch",
        python_callable=end_batch_run,
        trigger_rule="all_done"
    )

start_batch >> dbt_run_gold_altitude_band >> dbt_test_gold_altitude_band >> end_batch
start_batch >> dbt_run_gold_grid_analysis >> dbt_test_gold_grid_analysis >> end_batch
start_batch >> dbt_run_gold_supersonic_flights >> dbt_test_gold_supersonic_flights >> end_batch
