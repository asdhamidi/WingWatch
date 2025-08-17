from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_pipeline_orchestration',
    default_args=default_args,
    description='Orchestrate the data pipeline: raw->bronze->silver->gold',
    schedule_interval=None,
    catchup=False,
    tags=['data', 'pipeline', 'orchestration'],
) as dag:

    # Trigger the raw data ingestion DAG
    trigger_raw_ingestion = TriggerDagRunOperator(
        task_id='trigger_raw_minio_data_ingestion',
        trigger_dag_id='raw_minio_data_ingestion',
        wait_for_completion=True,
        poke_interval=30,
        execution_date='{{ ds }}',
        reset_dag_run=True,
    )

    # Trigger the bronze data ingestion DAG (depends on raw completion)
    trigger_bronze_ingestion = TriggerDagRunOperator(
        task_id='trigger_bronze_postgres_data_ingestion',
        trigger_dag_id='bronze_postgres_data_ingestion',
        wait_for_completion=True,
        poke_interval=30,
        execution_date='{{ ds }}',
        reset_dag_run=True,
    )

    # Trigger the silver deployment DAG (depends on bronze completion)
    trigger_silver_deployment = TriggerDagRunOperator(
        task_id='trigger_silver_postgres_deployment',
        trigger_dag_id='silver_postgres_deployment',
        wait_for_completion=True,
        poke_interval=30,
        execution_date='{{ ds }}',
        reset_dag_run=True,
    )

    # Trigger the gold processing DAG (depends on silver completion)
    trigger_gold_processing = TriggerDagRunOperator(
        task_id='trigger_gold_realtime_flights',
        trigger_dag_id='gold_realtime_flights',
        wait_for_completion=True,
        poke_interval=30,
        execution_date='{{ ds }}',
        reset_dag_run=True,
    )

    # Set up the dependencies
    trigger_raw_ingestion >> trigger_bronze_ingestion >> trigger_silver_deployment >> trigger_gold_processing
