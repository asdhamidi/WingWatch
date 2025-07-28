import os
import sys
from pathlib import Path
import logging
from io import BytesIO
from minio import Minio
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).parent))
from fetch_data import ( fetch_airlines_data, fetch_airports_data, fetch_cities_data, fetch_opensky_data, fetch_weather_data)

# Load environment variables from .env file
load_dotenv()

def ingest_data(data_type: str):
    """
    Ingests data based on the specified data_type and uploads it to the corresponding MinIO bucket.

    Args:
        data_type (str): Type of data to ingest. One of 'airlines', 'airports', 'cities', 'opensky', 'weather'.
    """
    data_map = {
        "airlines.json": fetch_airlines_data,
        "airports.json": fetch_airports_data,
        "cities.json": fetch_cities_data,
        "opensky.json": fetch_opensky_data,
        "weather.json": fetch_weather_data,
    }

    if data_type not in data_map:
        logging.error(f"Unknown data_type: {data_type}")
        raise ValueError(f"Unknown data_type: {data_type}")

    data = data_map[data_type]()
    put_on_bucket("raw", f"{data_type}", data)

def put_on_bucket(bucket_name: str, object_name: str, data: str) -> bool:
    """
    Uploads data to a MinIO bucket as an object.

    Args:
        bucket_name (str): Name of the MinIO bucket.
        object_name (str): Name of the object to create in the bucket.
        data (str): Data to upload as a string.

    Returns:
        bool: True if upload is successful.

    Raises:
        Exception: If upload fails.
    """
    logging.info(f"Starting upload to {bucket_name}/{object_name}...")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")

    try:
        # Connect to MinIO server
        logging.info("Connecting to MinIO...")
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        logging.info("MinIO connection established")

        # Ensure bucket exists, create if not
        logging.info(f"Checking if {bucket_name} exists...")
        if not minio_client.bucket_exists(bucket_name):
            logging.info(f"Creating bucket {bucket_name}...")
            minio_client.make_bucket(bucket_name)
        else:
            logging.info(f"{bucket_name} found!")

        data_stream = BytesIO(data.encode("utf-8"))

        # Upload object to MinIO
        result = minio_client.put_object(
            bucket_name,
            object_name,
            data=data_stream,
            length=len(data),
            content_type="application/json",
        )

        logging.info(f"Successfully uploaded {object_name} (etag: {result.etag})")
        return True

    except Exception as e:
        # Raise exception with context if upload fails
        raise Exception(
            f"Error in put_on_bucket. Upload failed for {object_name}: {str(e)}"
        )