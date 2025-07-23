from typing import Any, Dict, List
from datetime import datetime, timezone
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

def start_batch_run(hook: PostgresHook, batch_name: str) -> bool:
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
    if not batch_name:
        return False

    # Insert a new batch run with status 'STARTED'
    insert_query = f"""
        INSERT INTO admin.admin_batch_runs
        (BATCH_NAME, STATUS, BATCH_RUN_START_TIME)
        VALUES
        ('{batch_name}', 'STARTED', CURRENT_TIMESTAMP())
    """

    try:
        hook.run(insert_query)
        return True
    except Exception as e:
        raise AirflowException(f"Failed to insert batch run: {str(e)}")

def end_batch_run(hook: PostgresHook, batch_name: str) -> bool:
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
    if not batch_name:
        return False

    # Check if a batch run with status 'STARTED' exists
    check_query = f"""
        SELECT COUNT(*) FROM admin.admin_batch_runs
        WHERE BATCH_NAME = '{batch_name}' AND STATUS = 'STARTED'
    """

    try:
        count = hook.get_first(check_query)
        if not count or count[0] == 0:
            return False  # No active batch run found
    except Exception as e:
        raise AirflowException(f"Failed to check batch status: {str(e)}")

    # Update the batch run to 'COMPLETED'
    insert_query = f"""
        UPDATE admin.admin_batch_runs
        SET STATUS = 'COMPLETED', BATCH_RUN_END_TIME = CURRENT_TIMESTAMP()
        WHERE BATCH_NAME = '{batch_name}' AND STATUS = 'STARTED'
    """

    try:
        hook.run(insert_query)
        return True
    except Exception as e:
        raise AirflowException(f"Failed to update batch run: {str(e)}")

def start_job_run(hook: PostgresHook, batch_name: str, job_name: str) -> bool:
    """
    Inserts a new job run record into admin.admin_job_runs with status 'STARTED'.

    Args:
        hook (PostgresHook): Airflow Postgres hook for DB operations.
        job_name (str): Name of the job to start.

    Returns:
        bool: True if insertion succeeds, False if job_name is empty.

    Raises:
        AirflowException: If the insert fails.
    """
    if not job_name:
        return False

    # Insert a new job run with status 'STARTED'
    insert_query = f"""
        INSERT INTO admin.admin_job_runs
        (BATCH_NAME, JOB_NAME, STATUS, JOB_RUN_START_TIME)
        VALUES
        ('{batch_name}', '{job_name}', 'STARTED', CURRENT_TIMESTAMP())
    """

    try:
        hook.run(insert_query)
        return True
    except Exception as e:
        raise AirflowException(f"Failed to insert job run: {str(e)}")

def end_job_run(hook: PostgresHook, batch_name: str, job_name: str) -> bool:
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
    if not job_name:
        return False

    # Check if a job run with status 'STARTED' exists
    check_query = f"""
        SELECT COUNT(*) FROM admin.admin_job_runs
        WHERE JOB_NAME = '{job_name}' AND STATUS = 'STARTED'
    """

    try:
        count = hook.get_first(check_query)
        if not count or count[0] == 0:
            return False  # No active job run found
    except Exception as e:
        raise AirflowException(f"Failed to check job status: {str(e)}")

    # Update the job run to 'COMPLETED'
    insert_query = f"""
        UPDATE admin.admin_job_runs
        SET STATUS = 'COMPLETED', JOB_RUN_END_TIME = CURRENT_TIMESTAMP()
        WHERE JOB_NAME = '{job_name}' AND STATUS = 'STARTED'
    """

    try:
        hook.run(insert_query)
        return True
    except Exception as e:
        raise AirflowException(f"Failed to update job run: {str(e)}")