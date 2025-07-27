import logging
from typing import List, Dict, Any
from datetime import datetime, timezone
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

def insert_dq_results(hook: PostgresHook, dq_results: List[Dict[str, Any]]) -> None:
    """
    Inserts data quality (DQ) check results into the admin.admin_dq_results table.

    Args:
        hook (PostgresHook): Airflow Postgres hook for DB connection.
        dq_results (List[Dict[str, Any]]): List of DQ result dicts to insert.
    """
    if not dq_results:
        return

    insert_query = """
        INSERT INTO admin.admin_dq_results
        (id, result, execution_date, comments)
        VALUES
    """
    # Build values for bulk insert
    for r in dq_results:
        insert_query += f"({r['id']}, '{r['result']}', '{r['execution_date']}', '{r['comments']}'),\n"

    insert_query = insert_query.rstrip(",\n") + ";"

    try:
        hook.run(insert_query)
    except Exception as e:
        raise AirflowException(f"Failed to insert DQ results: {str(e)}")

def run_dq_checks(table_schema: str, table_name: str) -> None:
    """
    Executes all active DQ checks for a given table and records results.

    Args:
        table_schema (str): Schema name of the target table.
        table_name (str): Table name to run checks on.

    Raises:
        AirflowException: If any DQ check fails or DB errors occur.
    """
    logging.info(f"Executing DQ checks for table {table_schema}.{table_name}")
    pg_hook = PostgresHook(postgres_conn_id='hisaab_postgres')
    execution_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f%z")[:-2]
    results = []

    # Retrieve configured checks for this table
    retrieval_query = f"""
        SELECT id, type, target_column, range_check, lower_range, upper_range
        FROM admin.admin_dq_checks
        WHERE target_schema = '{table_schema.upper()}'
          AND target_table = '{table_name.upper()}'
          AND active = true
    """

    try:
        checks = pg_hook.get_records(retrieval_query)
        failed_flag = 0

        for check in checks:
            check_id, check_type, column, range_check, lower, upper = check

            # Dispatch to appropriate check function
            if range_check:
                result = _execute_range_check(pg_hook, table_schema, table_name, column, lower, upper)
            elif check_type == "NULL":
                result = _execute_null_check(pg_hook, table_schema, table_name, column)
            elif check_type == "UNIQUE":
                result = _execute_unique_check(pg_hook, table_schema, table_name, column)
            elif check_type == "DUPLICATE":
                result = _execute_duplicate_check(pg_hook, table_schema, table_name, column)
            else:
                result = "UNSUPPORTED_CHECK_TYPE"

            if result != "SUCCESS":
                failed_flag = 1
                print(f"Check {check_id} failed")

            results.append({
                'id': check_id,
                'result': result,
                'execution_date': execution_date,
                'comments': f"{check_type} check on {table_schema}.{table_name}.{column}"
            })

    except Exception as e:
        raise AirflowException(f"DQ check failed: {str(e)}")

    insert_dq_results(pg_hook, results)

    if failed_flag:
        raise AirflowException(f"DQ checks failed for {table_schema}.{table_name}")

def _execute_range_check(hook: PostgresHook, schema: str, table: str, column: str,
                        lower: float, upper: float) -> str:
    """
    Checks if column values are within the specified numeric range.

    Args:
        hook (PostgresHook): DB connection hook.
        schema (str): Table schema.
        table (str): Table name.
        column (str): Column to check.
        lower (float): Lower bound.
        upper (float): Upper bound.

    Returns:
        str: "SUCCESS" if all values are in range, else violation message.
    """
    query = f"""
        SELECT COUNT(*)
        FROM {schema}.{table}
        WHERE {column} < {lower} OR {column} > {upper}
    """
    invalid_count = hook.get_first(query)[0]
    return f"RANGE_VIOLATION ({invalid_count} records)" if invalid_count > 0 else "SUCCESS"

def _execute_null_check(hook: PostgresHook, schema: str, table: str, column: str) -> str:
    """
    Checks for NULL values in the specified column.

    Args:
        hook (PostgresHook): DB connection hook.
        schema (str): Table schema.
        table (str): Table name.
        column (str): Column to check.

    Returns:
        str: "SUCCESS" if no NULLs, else violation message.
    """
    query = f"""
        SELECT COUNT(*)
        FROM {schema}.{table}
        WHERE {column} IS NULL
    """
    null_count = hook.get_first(query)[0]
    return f"NULL_VIOLATION ({null_count} records)" if null_count > 0 else "SUCCESS"

def _execute_unique_check(hook: PostgresHook, schema: str, table: str, column: str) -> str:
    """
    Checks if all values in the column are unique.

    Args:
        hook (PostgresHook): DB connection hook.
        schema (str): Table schema.
        table (str): Table name.
        column (str): Column to check.

    Returns:
        str: "SUCCESS" if all values are unique, else violation message.
    """
    query = f"""
        SELECT COUNT(*) - COUNT(DISTINCT {column})
        FROM {schema}.{table}
    """
    duplicate_count = hook.get_first(query)[0]
    return f"UNIQUENESS_VIOLATION ({duplicate_count} duplicates)" if duplicate_count > 0 else "SUCCESS"

def _execute_duplicate_check(hook: PostgresHook, schema: str, table: str, column: str) -> str:
    """
    Checks for duplicate rows based on the specified column.

    Args:
        hook (PostgresHook): DB connection hook.
        schema (str): Table schema.
        table (str): Table name.
        column (str): Column to check.

    Returns:
        str: "SUCCESS" if no duplicates, else violation message.
    """
    query = f"""
        SELECT COUNT(*)
        FROM (
            SELECT {column}, COUNT(*)
            FROM {schema}.{table}
            GROUP BY {column}
            HAVING COUNT(*) > 1
        ) AS duplicates
    """
    duplicate_count = hook.get_first(query)[0]
    return f"DUPLICATE_ROWS ({duplicate_count} groups)" if duplicate_count > 0 else "SUCCESS"
