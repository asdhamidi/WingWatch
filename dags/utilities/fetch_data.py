import os
import csv
import requests
from json import dumps
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv
from typing import Any, Dict, List

# Load environment variables from .env file
load_dotenv()

OPENSKY_BASE_URL = "https://opensky-network.org/api"

def fetch_opensky_data(bbox: List[float]=[]) -> str:
    """
    Fetches live flight data from OpenSky Network API.

    Args:
        bbox (list, optional): [lamin, lomin, lamax, lomax] bounding box for filtering.

    Returns:
        str: JSON string containing timestamp and flight states.

    Raises:
        Exception: If request fails.
    """
    try:
        # Prepare bounding box parameters if provided
        params = (
            {"lamin": bbox[0], "lomin": bbox[1], "lamax": bbox[2], "lomax": bbox[3]}
            if bbox
            else {}
        )

        URL = f"{OPENSKY_BASE_URL}/states/all"
        response = requests.get(URL, params=params, timeout=10)
        response.raise_for_status()

        timestamp = datetime.now().isoformat()
        # Only return the 'states' field from response
        return dumps({"timestamp": timestamp, "data": response.json()["states"]})
    except requests.RequestException as e:
        raise Exception(f"Error fetching OpenSky data: {e}")

def fetch_airlines_data() -> str:
    """
    Fetches airline data from a public GitHub repository.

    Returns:
        str: JSON string of airlines data.

    Raises:
        Exception: If request fails.
    """
    try:
        URL = "https://raw.githubusercontent.com/asdhamidi/Airlines/refs/heads/master/airlines.json"
        response = requests.get(URL)
        response.raise_for_status()

        return dumps(response.json())
    except requests.RequestException as e:
        raise Exception(f"Error fetching airlines data: {e}")

def fetch_airports_data() -> str:
    """
    Fetches airport data in CSV format and returns it as a JSON string.

    Returns:
        str: JSON string of airports data.

    Raises:
        Exception: If request fails.
    """
    try:
        URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
        response = requests.get(URL)
        response.raise_for_status()
        csv_content = response.text

        # Convert CSV to list of dicts
        csv_file = StringIO(csv_content)
        reader = csv.DictReader(csv_file)
        airports_list = list(reader)

        # Convert to JSON string
        return dumps(airports_list)
    except requests.RequestException as e:
        raise Exception(f"Error fetching airports data: {e}")

def fetch_cities_data() -> str:
    """
    Fetches city data from a public GitHub repository.

    Returns:
        str: JSON string of cities data.

    Raises:
        Exception: If request fails.
    """
    try:
        URL = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/cities.json"
        response = requests.get(URL)
        response.raise_for_status()

        return dumps(response.json())

    except requests.RequestException as e:
        raise Exception(f"Error fetching cities data: {e}")

def fetch_weather_data(lat: float, lon: float) -> str:
    """
    Fetches current weather data for given latitude and longitude using OpenWeather API.

    Args:
        lat (float): Latitude.
        lon (float): Longitude.

    Returns:
        str: JSON string of weather data.

    Raises:
        Exception: If request fails.
    """
    api_key = os.getenv("OPENWEATHER_API_KEY")
    try:
        URL = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
        response = requests.get(URL)
        response.raise_for_status()
        return dumps(response.json())

    except requests.RequestException as e:
        raise Exception(f"Error fetching weather data: {e}")
