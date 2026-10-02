"""
Extract step: pull current + hourly forecast data from Open-Meteo for a given
city and write the raw JSON payload to disk, timestamped.

Open-Meteo requires no API key. Docs: https://open-meteo.com/en/docs
"""
import json
import os
from datetime import datetime, timezone

import requests

CITIES = {
    "winnipeg": {"lat": 49.8951, "lon": -97.1384},
}

RAW_DATA_DIR = os.environ.get("RAW_DATA_DIR", "/opt/airflow/data/raw")

API_URL = "https://api.open-meteo.com/v1/forecast"


def extract_city(city: str) -> str:
    """Fetch weather data for one city and save it as a raw JSON file.

    Returns the path to the file written.
    """
    coords = CITIES[city]
    params = {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "hourly": "temperature_2m,precipitation,wind_speed_10m,weather_code",
        "timezone": "America/Winnipeg",
        "forecast_days": 1,
        "past_days": 1,
    }

    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(RAW_DATA_DIR, f"{city}_{run_ts}.json")

    with open(out_path, "w") as f:
        json.dump({"city": city, "fetched_at": run_ts, "payload": payload}, f)

    return out_path


def extract_all() -> list[str]:
    return [extract_city(city) for city in CITIES]


if __name__ == "__main__":
    paths = extract_all()
    print(f"Wrote {len(paths)} raw file(s): {paths}")
