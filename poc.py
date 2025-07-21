import requests
from typing import List, Dict, Any
from datetime import datetime

BASE_URL = "https://opensky-network.org/api"
STATES = [
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

def get_flights_data() -> Dict[str, Any]:
    try:
        response = requests.get(f"{BASE_URL}/states/all")
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching flight data: {e}")
        return {}

def process_flights_data(flights_data: Dict[str, Any]) -> None:
    captureTime = int(flights_data.get("time", 0))
    captureTimeFormatted = datetime.fromtimestamp(captureTime).strftime('%Y-%m-%d %H:%M:%S')
    print(f"Capture Time: {captureTimeFormatted}")

    no_of_flights = len(flights_data.get("states", []))
    print(f"Number of Flights: {no_of_flights}")

process_flights_data(get_flights_data())