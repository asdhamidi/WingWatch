import os
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Root directory containing SQL scripts organized by layer
DDL_ROOT = "./sql_scripts"

def get_ddl_files(layer):
    """
    Reads all .sql files from the specified layer directory and returns their contents as a list of strings.
    Each SQL file's content is stripped of newlines for compatibility with Airflow's SQL operator.
    """
    file_list = []
    for f in os.listdir(f"{DDL_ROOT}/{layer}"):
        if f.endswith(".sql"):
            with open(f"{DDL_ROOT}/{layer}/{f}") as f:
                file_list.append(f.read().replace("\n", ""))
    return file_list

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
}

# Define the DAG for schema deployment
with DAG(
    "postgres_ddl_deployment",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    tags=["postgres"],
    catchup=False,
) as dag:
    deploy_admin_schema = PostgresOperator.partial(
        task_id="deploy_admin_schema",
        postgres_conn_id="postgres_conn",
        map_index_template="{{(task.sql[task.sql.find('CREATE TABLE')+12:task.sql.find('(')])}}",
    ).expand(sql=get_ddl_files("admin"))

    deploy_admin_dml_schema = PostgresOperator.partial(
        task_id="deploy_admin_dml_schema",
        postgres_conn_id="postgres_conn",
        map_index_template="{{(task.sql[task.sql.find('TRUNCATE TABLE')+14:task.sql.find(';')])}}",
    ).expand(sql=get_ddl_files("admin_dml"))

    deploy_bronze_schema = PostgresOperator.partial(
        task_id="deploy_bronze_schema",
        postgres_conn_id="postgres_conn",
        map_index_template="{{(task.sql[task.sql.find('CREATE TABLE')+12:task.sql.find('(')])}}",
    ).expand(sql=get_ddl_files("bronze"))

    deploy_silver_schema = PostgresOperator.partial(
        task_id="deploy_silver_schema",
        postgres_conn_id="postgres_conn",
        map_index_template="{{(task.sql[task.sql.find('CREATE TABLE')+12:task.sql.find('(')])}}",
    ).expand(sql=get_ddl_files("silver"))

    deploy_gold_schema = PostgresOperator.partial(
        task_id="deploy_gold_schema",
        postgres_conn_id="postgres_conn",
        map_index_template="{{(task.sql[task.sql.find('CREATE TABLE')+12:task.sql.find('(')])}}",
    ).expand(sql=get_ddl_files("gold"))

deploy_admin_schema >> deploy_admin_dml_schema >> [deploy_bronze_schema, deploy_silver_schema, deploy_gold_schema]
