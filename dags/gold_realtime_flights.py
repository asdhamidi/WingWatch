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
    "gold_realtime_flights",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    tags=["aviation", "gold", "realtime_flights"],
    catchup=False,
    schedule="@hourly"
) as dag:
    start_batch=PythonOperator(
        task_id="start_batch",
        python_callable=start_batch_run,
    )

    dbt_run_gold_approaching_airports = BashOperator(
        task_id='dbt_run_gold_approaching_airports',
        bash_command=(
            'cd /usr/app/dbt && '
            'dbt run --select gold.gold_approaching_airports'
        ),
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    dbt_test_gold_approaching_airports = PythonOperator(
        task_id='dbt_test_gold_approaching_airports',
        python_callable=run_dq_checks,
        op_kwargs={
            'table_schema': 'gold',
            'table_name': 'gold_approaching_airports'
        }
    )

    dbt_run_gold_flight_phase = BashOperator(
        task_id='dbt_run_gold_flight_phase',
        bash_command=(
            'cd /usr/app/dbt && '
            'dbt run --select gold.gold_flight_phase'
        ),
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    dbt_test_gold_flight_phase = PythonOperator(
        task_id='dbt_test_gold_flight_phase',
        python_callable=run_dq_checks,
        op_kwargs={
            'table_schema': 'gold',
            'table_name': 'gold_flight_phase'
        }
    )

    dbt_run_gold_peak_traffic_hours = BashOperator(
        task_id='dbt_run_gold_peak_traffic_hours',
        bash_command=(
            'cd /usr/app/dbt && '
            'dbt run --select gold.gold_peak_traffic_hours'
        ),
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    dbt_test_gold_peak_traffic_hours = PythonOperator(
        task_id='dbt_test_gold_peak_traffic_hours',
        python_callable=run_dq_checks,
        op_kwargs={
            'table_schema': 'gold',
            'table_name': 'gold_peak_traffic_hours'
        }
    )

    dbt_run_gold_emergency_events = BashOperator(
        task_id='dbt_run_gold_emergency_events',
        bash_command=(
            'cd /usr/app/dbt && '
            'dbt run --select gold.gold_emergency_events'
        ),
        pre_execute=start_job_run,
        on_success_callback=end_job_run,
        on_failure_callback=end_job_run
    )

    dbt_test_gold_emergency_events = PythonOperator(
        task_id='dbt_test_gold_emergency_events',
        python_callable=run_dq_checks,
        op_kwargs={
            'table_schema': 'gold',
            'table_name': 'gold_emergency_events'
        }
    )

    end_batch = PythonOperator(
        task_id="end_batch",
        python_callable=end_batch_run,
        trigger_rule="all_done"
    )

start_batch >> dbt_run_gold_approaching_airports >> dbt_test_gold_approaching_airports >> end_batch
start_batch >> dbt_run_gold_flight_phase >> dbt_test_gold_flight_phase >> end_batch
start_batch >> dbt_run_gold_peak_traffic_hours >> dbt_test_gold_peak_traffic_hours >> end_batch
start_batch >> dbt_run_gold_emergency_events >> dbt_test_gold_emergency_events >> end_batch
