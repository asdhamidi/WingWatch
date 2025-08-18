import logging
import subprocess
from typing import List, Dict, Any
from airflow.exceptions import AirflowException

def run_dq_checks(table_schema: str, table_name: str) -> None:
    """
    Runs dbt tests for the specified table and inserts the results into the admin.admin_dq_results table.

    Args:
        table_schema (str): The schema of the table to test.
        table_name (str): The name of the table to test.

    Raises:
        AirflowException: If dbt test fails or results cannot be inserted.
    """
    logging.info(f"Running DQ checks for {table_schema}.{table_name}")
    # Build dbt test command for the specific table
    dbt_cmd = [
        "dbt",
        "test",
        "--select",
        f"{table_name}"
    ]

    # Run dbt test command in the dbt project directory
    logging.info(f"Running dbt command: {' '.join(dbt_cmd)}")
    try:
        result = subprocess.run(
            dbt_cmd,
            cwd="/usr/app/dbt",
            capture_output=True,
            text=True,
            check=True
        )

        if result.returncode != 0:
            raise AirflowException(f"dbt test failed with return code {result.returncode}")
        logging.info("DQ checks completed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"dbt test failed: {e.stderr}")
        raise AirflowException(f"dbt test failed: {e.stderr}")
