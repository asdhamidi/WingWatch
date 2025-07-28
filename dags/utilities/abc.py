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

def end_batch_run(**kwargs) -> bool:
    """
    End a batch run by updating its status to 'COMPLETED' or 'FAILED' in admin.admin_batch_runs.

    Args:
        kwargs: Airflow context dictionary. Must contain 'dag'.

    Returns:
        bool: True if update succeeds.

    Raises:
        AirflowException: If no active batch found or DB operation fails.
    """
    try:
        if not kwargs or 'dag' not in kwargs:
            raise AirflowException("Kwargs does not contain required key 'dag'")
        
        batch_name = kwargs.get('dag').dag_id

        if not batch_name:
            raise AirflowException("Missing batch name in kwargs.")
        
        logging.info(f"Ending batch run for {batch_name}...")

        hook = PostgresHook(postgres_conn_id='postgres_conn')

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

        # Retrieve Batch ID for the current batch
        logging.info(f"Retrieving Batch Run ID for batch '{batch_name}'...")
        BRI_query = f"""
            SELECT BATCH_RUN_ID FROM ADMIN.ADMIN_BATCH_RUNS
            WHERE BATCH_NAME = '{batch_name}' AND BATCH_STATUS = 'STARTED'
        """

        batch_run_id = hook.get_first(BRI_query)

        if not batch_run_id or batch_run_id[0] is None:
            raise AirflowException(f"No active batch found for '{batch_name}'. Cannot end batch.")
        
        batch_run_id = batch_run_id[0]
        logging.info(f"Batch ID for {batch_name} is {batch_run_id}.")

        # Check if any job in this batch is still running or failed
        logging.info(f"Checking for active or failed jobs in batch '{batch_name}'...")
        jobs_check_query = f"""
            SELECT COUNT(*) FROM ADMIN.ADMIN_JOB_RUNS
            WHERE BATCH_NAME = '{batch_name}' AND BATCH_RUN_ID = {batch_run_id} AND (JOB_STATUS = 'STARTED' OR JOB_STATUS='FAILED')
        """

        count = hook.get_first(jobs_check_query)
        if not count or count[0] != 0:
            # If jobs are still running or failed, mark batch as FAILED
            insert_query = f"""
            UPDATE admin.admin_batch_runs
            SET BATCH_STATUS = 'FAILED', BATCH_RUN_END_TIME = CURRENT_TIMESTAMP
            WHERE BATCH_NAME = '{batch_name}' AND BATCH_RUN_ID = {batch_run_id} AND BATCH_STATUS = 'STARTED'
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
            WHERE BATCH_NAME = '{batch_name}' AND BATCH_RUN_ID = {batch_run_id} AND BATCH_STATUS = 'STARTED'
        """
        hook.run(insert_query)
        logging.info(f"Batch run for {batch_name} ended successfully.")

        return True
    except Exception as e:
        raise AirflowException(f"Error in executing stop_batch_run: {str(e)}")

def start_job_run(context: Dict[str, Any]) -> bool:
    """
    Start a new job run by inserting a record into ADMIN.ADMIN_JOB_RUNS with status 'STARTED'.

    Args:
        context: Airflow context dictionary. Must contain 'dag' and 'task'.

    Returns:
        bool: True if insertion succeeds.

    Raises:
        AirflowException: If job is already running or DB operation fails.
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

        # Retrieve Batch ID for the current batch
        logging.info(f"Retrieving Batch ID for batch '{batch_name}'...")
        BRI_query = f"""
            SELECT BATCH_RUN_ID FROM ADMIN.ADMIN_BATCH_RUNS
            WHERE BATCH_NAME = '{batch_name}' AND BATCH_STATUS = 'STARTED'
        """

        batch_run_id = hook.get_first(BRI_query)
        if not batch_run_id or batch_run_id[0] is None:
            raise AirflowException(f"No active batch found for '{batch_name}'. Cannot start job.")
        batch_run_id = batch_run_id[0]
        logging.info(f"Batch ID for {batch_name} is {batch_run_id}.")

        # Check if a job run with status 'STARTED' exists
        logging.info(f"Checking if job '{job_name}' is already running...")
        check_query = f"""
            SELECT COUNT(*) FROM ADMIN.ADMIN_JOB_RUNS
            WHERE JOB_NAME = '{job_name}' AND BATCH_RUN_ID = {batch_run_id} AND JOB_STATUS = 'STARTED'
        """

        job_count = hook.get_first(check_query)
        if not job_count or job_count[0] != 0:
            raise AirflowException(f"Job '{job_name}' is already running.")
        
        logging.info(f"No active job run found for {job_name}.")

        # Insert a new job run with status 'STARTED'
        logging.info(f"Inserting new job run for {job_name}...")
        insert_query = f"""
            INSERT INTO ADMIN.ADMIN_JOB_RUNS
            (BATCH_NAME, BATCH_RUN_ID, JOB_NAME, JOB_STATUS, JOB_RUN_START_TIME)
            VALUES
            ('{batch_name}', {batch_run_id}, '{job_name}', 'STARTED', CURRENT_TIMESTAMP)
        """
        hook.run(insert_query)
        logging.info(f"Job run for {job_name} started successfully.")

        return True
    except Exception as e:
        raise AirflowException(f"Error in executing start_job_run: {str(e)}")

def end_job_run(context: Dict[str, Any]) -> bool:
    """
    End a job run by updating its status and end time in ADMIN.ADMIN_JOB_RUNS.

    Args:
        context: Airflow context dictionary. Must contain 'task' and 'task_instance'.

    Returns:
        bool: True if update succeeds.

    Raises:
        AirflowException: If no active job found or DB operation fails.
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
        logging.info(f"Checking if job '{job_name}' is running...")
        check_query = f"""
            SELECT COUNT(*) FROM ADMIN.ADMIN_JOB_RUNS
            WHERE JOB_NAME = '{job_name}' AND JOB_STATUS = 'STARTED'
        """

        count = hook.get_first(check_query)
        if not count or count[0] == 0:
            raise AirflowException(f"No active job found for '{job_name}'. Cannot end job.")
        logging.info(f"Active job run found for {job_name}.")

        # Retrieve Job Run ID for the current job
        logging.info(f"Retrieving Job Run ID for job '{job_name}'...")
        JRI_query = f"""
            SELECT JOB_RUN_ID FROM ADMIN.ADMIN_JOB_RUNS
            WHERE JOB_NAME = '{job_name}' AND JOB_STATUS = 'STARTED'
        """

        job_run_id = hook.get_first(JRI_query)
        if not job_run_id or job_run_id[0] == 0:
            raise AirflowException(f"No active job run found for {job_name}. Cannot end job.")
        job_run_id = job_run_id[0]
        logging.info(f"Job Run ID for {job_name} is {job_run_id}.")

        # Update the job run to the final status
        logging.info(f"Updating job run status for {job_name} to '{job_status}'...")
        insert_query = f"""
            UPDATE ADMIN.ADMIN_JOB_RUNS
            SET JOB_STATUS = UPPER('{job_status}'), JOB_RUN_END_TIME = CURRENT_TIMESTAMP
            WHERE JOB_NAME = '{job_name}' AND JOB_RUN_ID = {job_run_id} AND JOB_STATUS = 'STARTED'
        """
        hook.run(insert_query)
        logging.info(f"Job run for {job_name} ended successfully with status '{job_status}'.")

        return True
    except Exception as e:
        raise AirflowException(f"Error in executing end_job_run: {str(e)}")
