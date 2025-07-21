import os
import logging
import requests
from io import BytesIO
from minio import Minio
from typing import Any, Dict, List
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

OPENSKY_BASE_URL = "https://opensky-network.org/api"

def put_on_bucket(bucket_name: str, object_name: str, data: str) -> bool:
    """
    Uploads data to a MinIO bucket as an object.

    Args:
        bucket_name (str): Name of the MinIO bucket.
        object_name (str): Name of the object to create in the bucket.
        data (str): Data to upload (JSON string).

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
        # Connect to MinIO
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

        # Convert data to BytesIO for upload
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
        raise Exception(f"Error in put_on_bucker. Upload failed for {object_name}: {str(e)}")

def fetch_opensky_data(bbox=None) -> Dict[str, Any]:
    try:
        params = {"lamin": bbox[0], "lomin": bbox[1], "lamax": bbox[2], "lomax": bbox[3]} if bbox else {}
        
        URL = f"{OPENSKY_BASE_URL}/states/all"
        response = requests.get(URL, params=params, timeout=10)
        response.raise_for_status()
        
        timestamp = datetime.now().isoformat()
        return {
            "timestamp": timestamp,
            "data": response.json()["states"]
        }
    except requests.RequestException as e:
        raise Exception(f"Error fetching OpenSky data: {e}")

def fetch_airlines_data() -> List[Dict[str, Any]]:
    try:
        URL = "https://raw.githubusercontent.com/asdhamidi/Airlines/refs/heads/master/airlines.json"
        response = requests.get(URL)
        response.raise_for_status()

        return response.json()
    
    except requests.RequestException as e:
        raise Exception(f"Error fetching airlines data: {e}")
    
def fetch_airports_data() -> bytes:
    try:
        URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
        response = requests.get(URL)
        response.raise_for_status()

        return response.content
    
    except requests.RequestException as e:
        raise Exception(f"Error fetching airports data: {e}")

def fetch_cities_data() -> List[Dict[str, Any]]:
    try:
        URL = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/cities.json"
        response = requests.get(URL)
        response.raise_for_status()

        return response.json()
    
    except requests.RequestException as e:
        raise Exception(f"Error fetching cities data: {e}")

def fetch_weather_data(lat: float, lon: float) -> Dict[str, Any]:
    api_key = os.getenv("OPENWEATHER_API_KEY")
    try:
        URL = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
        response = requests.get(URL)
        response.raise_for_status()
        return response.json()
    
    except requests.RequestException as e:
        raise Exception(f"Error fetching weather data: {e}")
    