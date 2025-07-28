import logging
from typing import Any, Dict, List
from datetime import datetime, timezone
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

def start_batch_run(**kwargs) -> bool:
    """
    Start a new batch run by inserting a record into admin.admin_batch_runs with status 'STARTED'.

    Args:
        kwargs: Airflow context dictionary. Must contain 'dag'.

    Returns:
        bool: True if insertion succeeds.

    Raises:
        AirflowException: If batch is already running or DB operation fails.
    """
    try:
        if not kwargs or 'dag' not in kwargs:
            raise AirflowException("Kwargs does not contain required key 'dag'")

        batch_name = kwargs.get('dag').dag_id

        if not batch_name:
            raise AirflowException("Missing batch name in kwargs.")

        logging.info(f"Starting batch run for {batch_name}...")
        hook = PostgresHook(postgres_conn_id='postgres_conn')

        # Check if a batch run with status 'STARTED' exists
        logging.info(f"Checking if batch '{batch_name}' is already running...")
        check_query = f"""
            SELECT COUNT(*) FROM admin.admin_batch_runs
            WHERE BATCH_NAME = '{batch_name}' AND BATCH_STATUS = 'STARTED'
        """

        count = hook.get_first(check_query)
        if not count or count[0] != 0:
            raise AirflowException(f"Batch '{batch_name}' is already running.")
        logging.info(f"No active batch run found for {batch_name}.")

        # Insert a new batch run with status 'STARTED'
        logging.info(f"Inserting new batch run for {batch_name}...")
        insert_query = f"""
            INSERT INTO admin.admin_batch_runs
            (BATCH_NAME, BATCH_STATUS, BATCH_RUN_START_TIME)
            VALUES
            ('{batch_name}', 'STARTED', CURRENT_TIMESTAMP)
        """
        hook.run(insert_query)
        logging.info(f"Batch run for {batch_name} started successfully.")

        return True
    except Exception as e:
        raise AirflowException(f"Error in executing start_batch_run: {str(e)}")
