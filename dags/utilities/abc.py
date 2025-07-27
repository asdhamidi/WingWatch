import logging
from typing import Any, Dict, List
from datetime import datetime, timezone
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

def start_batch_run(batch_name: str) -> bool:
    """
    Inserts a new batch run record into admin.admin_batch_runs with status 'STARTED'.

    Args:
        hook (PostgresHook): Airflow Postgres hook for DB operations.
        batch_name (str): Name of the batch to start.

    Returns:
        bool: True if insertion succeeds, False if batch_name is empty.

    Raises:
        AirflowException: If the insert fails.
    """
    try:
        logging.info(f"Starting batch run for {batch_name}...")
        hook = PostgresHook(postgres_conn_id='postgres_conn')
        if not batch_name:
            return False

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

def end_batch_run(batch_name: str) -> bool:
    """
    Updates the batch run status to 'COMPLETED' and sets end time.

    Args:
        hook (PostgresHook): Airflow Postgres hook for DB operations.
        batch_name (str): Name of the batch to end.

    Returns:
        bool: True if update succeeds, False if no active batch found.

    Raises:
        AirflowException: If DB operations fail.
    """
    try:
        logging.info(f"Ending batch run for {batch_name}...")
        hook = PostgresHook(postgres_conn_id='postgres_conn')
        if not batch_name:
            return False

        # Check if a batch run with status 'STARTED' exists
        logging.info(f"Checking if batch '{batch_name}' is running...")
        batch_check_query = f"""
            SELECT COUNT(*) FROM admin.admin_batch_runs
            WHERE BATCH_NAME = '{batch_name}' AND BATCH_STATUS = 'STARTED'
        """

        count = hook.get_first(batch_check_query)
        if not count or count[0] == 0:
            raise AirflowException(f"No active batch found for '{batch_name}'. Cannot end batch.")
        logging.info(f"Active batch run found for {batch_name}.")

        # Check if a job has failed or is active for this batch
        logging.info(f"Checking for active or failed jobs in batch '{batch_name}'...")
        jobs_check_query = f"""
            SELECT COUNT(*) FROM ADMIN.ADMIN_JOB_RUNS
            WHERE BATCH_NAME = '{batch_name}' AND JOB_STATUS = 'STARTED' OR JOB_STATUS='FAILED'
        """

        count = hook.get_first(jobs_check_query)
        if not count or count[0] != 0:
            insert_query = f"""
            UPDATE admin.admin_batch_runs
            SET BATCH_STATUS = 'FAILED', BATCH_RUN_END_TIME = CURRENT_TIMESTAMP
            WHERE BATCH_NAME = '{batch_name}' AND BATCH_STATUS = 'STARTED'
            """
            hook.run(insert_query)
            logging.info(f"Batch run for {batch_name} ended with status 'FAILED'.")
            raise AirflowException(f"Batch '{batch_name}' has active/failed jobs. Cannot end batch.")
        logging.info(f"No active or failed jobs found for batch '{batch_name}'.")

        # Update the batch run to 'COMPLETED'
        logging.info(f"Updating batch run status for {batch_name} to 'COMPLETED'...")
        insert_query = f"""
            UPDATE admin.admin_batch_runs
            SET BATCH_STATUS = 'COMPLETED', BATCH_RUN_END_TIME = CURRENT_TIMESTAMP
            WHERE BATCH_NAME = '{batch_name}' AND BATCH_STATUS = 'STARTED'
        """
        hook.run(insert_query)
        logging.info(f"Batch run for {batch_name} ended successfully.")

        return True
    except Exception as e:
        raise AirflowException(f"Error in executing stop_batch_run: {str(e)}")

def start_job_run(context: Dict[str, Any]) -> bool:
    """
    Inserts a new job run record into ADMIN.ADMIN_JOB_RUNS with status 'STARTED'.

    Args:
        hook (PostgresHook): Airflow Postgres hook for DB operations.
        job_name (str): Name of the job to start.

    Returns:
        bool: True if insertion succeeds, False if job_name is empty.

    Raises:
        AirflowException: If the insert fails.
    """
    try:
        logging.info("Starting job run...")
        if not context or 'dag' not in context or 'task' not in context:
            raise AirflowException("Context does not contain required keys 'dag' or 'task'.")
        
        batch_name = context.get('dag').dag_id
        job_name = context.get('task').task_id

        if not job_name or not batch_name:
            raise AirflowException("Missing batch or job name in context.")
        
        hook = PostgresHook(postgres_conn_id='postgres_conn')

        # Check if a job run with status 'STARTED' exists
        logging.info(f"Checking if job '{job_name}' is already running...")
        check_query = f"""
            SELECT COUNT(*) FROM ADMIN.ADMIN_JOB_RUNS
            WHERE JOB_NAME = '{job_name}' AND JOB_STATUS = 'STARTED'
        """

        count = hook.get_first(check_query)
        if not count or count[0] != 0:
            raise AirflowException(f"Job '{job_name}' is already running.")
        logging.info(f"No active job run found for {job_name}.")

        # Insert a new job run with status 'STARTED'
        logging.info(f"Inserting new job run for {job_name}...")
        insert_query = f"""
            INSERT INTO ADMIN.ADMIN_JOB_RUNS
            (BATCH_NAME, JOB_NAME, JOB_STATUS, JOB_RUN_START_TIME)
            VALUES
            ('{batch_name}', '{job_name}', 'STARTED', CURRENT_TIMESTAMP)
        """
        hook.run(insert_query)
        logging.info(f"Job run for {job_name} started successfully.")

        return True
    except Exception as e:
        raise AirflowException(f"Error in executing start_job_run: {str(e)}")

def end_job_run(context: Dict[str, Any]) -> bool:
    """
    Updates the job run status to 'COMPLETED' and sets end time.

    Args:
        hook (PostgresHook): Airflow Postgres hook for DB operations.
        job_name (str): Name of the job to end.

    Returns:
        bool: True if update succeeds, False if no active job found.

    Raises:
        AirflowException: If DB operations fail.
    """
    try:
        logging.info("Ending job run...")
        if not context or 'dag' not in context or 'task' not in context:
            raise AirflowException("Context does not contain required keys 'dag' or 'task'.")
        
        job_name = context.get('task').task_id
        job_status = context.get('task_instance').state

        if not job_name:
            raise AirflowException("Missing job name in context.")
        
        hook = PostgresHook(postgres_conn_id='postgres_conn')

        # Check if a job run with status 'STARTED' exists
        logging.info(f"Checking if job '{job_name}' is already running...")
        check_query = f"""
            SELECT COUNT(*) FROM ADMIN.ADMIN_JOB_RUNS
            WHERE JOB_NAME = '{job_name}' AND JOB_STATUS = 'STARTED'
        """

        count = hook.get_first(check_query)
        if not count or count[0] == 0:
            raise AirflowException(f"No active job found for '{job_name}'. Cannot end job.")
        logging.info(f"Active job run found for {job_name}.")

        # Update the job run to 'COMPLETED'
        logging.info(f"Updating job run status for {job_name} to '{job_status}'...")
        insert_query = f"""
            UPDATE ADMIN.ADMIN_JOB_RUNS
            SET JOB_STATUS = UPPER('{job_status}'), JOB_RUN_END_TIME = CURRENT_TIMESTAMP
            WHERE JOB_NAME = '{job_name}' AND JOB_STATUS = 'STARTED'
        """
        hook.run(insert_query)
        logging.info(f"Job run for {job_name} ended successfully with status '{job_status}'.")

        return True
    except Exception as e:
        raise AirflowException(f"Error in executing end_job_run: {str(e)}")