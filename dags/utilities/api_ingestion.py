import os
import csv
import logging
import requests
from json import dumps
from io import StringIO
from dotenv import load_dotenv
from typing import List

# Load environment variables from .env file
load_dotenv()

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
        URL = "https://opensky-network.org/api/states/all"
        # Prepare bounding box parameters if provided
        params = (
            {"lamin": bbox[0], "lomin": bbox[1], "lamax": bbox[2], "lomax": bbox[3]}
            if bbox
            else {}
        )
        logging.info("Fetching OpenSky data...")
        response = requests.get(URL, params=params, timeout=10)
        response.raise_for_status()
        logging.info("OpenSky data fetched successfully.")
        logging.info("Processing OpenSky data...")
        STATE_HEADERS = [
            "icao24",
            "callsign",
            "origin_country",
            "time_position",
            "last_contact",
            "longitude",
            "latitude",
            "baro_altitude",
            "on_ground",
            "velocity",
            "true_track",
            "vertical_rate",
            "sensors",
            "geo_altitude",
            "squawk",
            "spi",
            "position_source",
            "category"
        ]

        res_data = response.json()["states"]

        data = []
        for state in res_data:
            data.append(dict(zip(STATE_HEADERS, state)))
        logging.info("OpenSky data processed successfully.")

        return dumps(data)
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
        logging.info("Fetching airlines data...")
        URL = "https://lsv.ens-paris-saclay.fr/~sirangel/teaching/dataset/airlines.txt"
        response = requests.get(URL)
        response.raise_for_status()
        logging.info("Airlines data fetched successfully.")

        logging.info("Processing airlines data...")
        data = "airline_name,iata_code,icao_code,callsign,country\n" + response.text.replace(";",",")
        data = data.replace("\\N", "")
        csv_file = StringIO(data)
        reader = csv.DictReader(csv_file)
        airlines_list = list(reader)
        logging.info("Airlines data processed successfully.")

        return dumps(airlines_list)
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
        logging.info("Fetching airports data...")
        URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
        response = requests.get(URL)
        response.raise_for_status()
        csv_content = response.text
        logging.info("Airports data fetched successfully.")

        # Convert CSV to list of dicts
        logging.info("Converting CSV to list of dicts...")
        csv_file = StringIO(csv_content)
        reader = csv.DictReader(csv_file)
        airports_list = list(reader)
        logging.info("CSV converted to list of dicts.")

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
        logging.info("Fetching cities data...")
        URL = "https://raw.githubusercontent.com/asdhamidi/countries-states-cities-database/refs/heads/master/json/cities.json"
        response = requests.get(URL)
        response.raise_for_status()
        logging.info("Cities data fetched successfully.")

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
