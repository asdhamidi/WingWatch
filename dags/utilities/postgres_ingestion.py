import os
import json
import logging
from minio import Minio
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def truncate_table(table_name: str) -> None:
    """
    Truncates a specified Postgres table.

    Args:
        table_name (str): Name of the Postgres table to truncate.
    """
    try:
        logging.info(f"Truncating table {table_name}...")
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        hook.run(f"TRUNCATE TABLE {table_name}")
        logging.info(f"Table {table_name} truncated successfully.")
    except Exception as e:
        raise Exception(f"Error truncating table {table_name}: {e}")

def minio_to_postgres(
    bucket_name: str,
    object_name: str,
    table_name: str
) -> None:
    """
    Fetches JSON data from a Minio bucket and inserts it into a Postgres table.

    Args:
        bucket_name (str): Name of the Minio bucket.
        object_name (str): Name of the object in the Minio bucket.
        table_name (str): Name of the Postgres table to insert data into.
    """
    data = get_json_from_minio(bucket_name, object_name)
    insert_data_to_postgres(data, table_name)

def get_json_from_minio(bucket_name: str, object_name: str):
    """
    Retrieves a JSON object from a specified Minio bucket and object.

    Args:
        bucket_name (str): The name of the Minio bucket.
        object_name (str): The name of the object (file) in the Minio bucket.

    Returns:
        dict: The JSON data loaded as a Python dictionary.

    Raises:
        Exception: If there is an error fetching or parsing the JSON from Minio.
    """
    try:
        # Load Minio credentials and endpoint from environment variables
        logging.info("Loading Minio credentials and endpoint from environment variables...")
        MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
        MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
        MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")

        # Initialize Minio client
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        # Fetch the object from Minio
        logging.info("Fetching object from Minio...")
        response = minio_client.get_object(bucket_name, object_name)
        json_bytes = response.read()
        response.close()
        logging.info("Object fetched successfully.")

        # Decode and parse JSON data
        logging.info("Decoding and parsing JSON data...")
        data = json.loads(json_bytes.decode('utf-8'))
        logging.info("JSON data decoded and parsed successfully.")

        # Validate if each dict in this list has the same number of keys
        logging.info("Validating JSON data...")
        if isinstance(data, list) and data:
            key_count = len(data[0].keys())
            for idx, item in enumerate(data):
                if not isinstance(item, dict):
                    raise Exception(f"Item at index {idx} is not a dictionary.")
                if len(item.keys()) != key_count:
                    raise Exception(f"Dictionary at index {idx} does not have the same number of keys as the first dictionary.")
        logging.info("JSON data validated successfully.")

        return data
    except Exception as e:
        raise Exception(f"Error fetching JSON from Minio: {e}")

def insert_data_to_postgres(data, table_name):
    """
    Inserts data into a specified Postgres table using Airflow's PostgresHook.

    Args:
        data (list or dict): The data to insert. Can be a list of dicts or a single dict.
        table_name (str): The name of the Postgres table to insert data into.

    Raises:
        Exception: If there is an error during the insert operation.
    """
    logging.info("Initializing data ingestion into Postgres...")
    hook = PostgresHook(postgres_conn_id="postgres_conn")

    # Ensure data is a list of dicts
    if isinstance(data, dict):
        data = [data]

    if not data:
        logging.info("No data to insert.")
        return

    truncate_table(table_name)
    try:
        # Extract columns and join names
        columns = list(data[0].keys())
        cols_str = ', '.join(columns)

        # Number of rows and columns
        n_rows = len(data)
        n_cols = len(columns)

        # Create placeholders for all rows e.g. (%s,%s,%s), (%s,%s,%s), ...
        placeholders = ', '.join(['(' + ', '.join(['%s'] * n_cols) + ')'] * n_rows)

        # Build insert query
        insert_query = f"INSERT INTO {table_name.upper()} ({cols_str}) VALUES {placeholders}"

        # Flatten the values into a single list for all rows
        values = []
        for row in data:
            for col in columns:
                if isinstance(row[col], str):
                    values.append(row[col].strip() if row[col].strip() != '' else None)
                else:
                    values.append(row[col])

        logging.info(f"Executing query: {insert_query}")

        hook.run(insert_query, parameters=values)

        logging.info("Data inserted successfully.")
    except Exception as e:
        raise Exception(f"Error inserting data into Postgres: {e}")
