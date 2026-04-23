import pandas as pd
from typing import Dict


def transform_hourly_weather(weather_json: Dict, location: Dict) -> pd.DataFrame:
    hourly = weather_json["hourly"]

    df = pd.DataFrame({
        "timestamp": hourly["time"],
        "temperature": hourly["temperature_2m"],
        "humidity": hourly["relative_humidity_2m"],
        "precipitation": hourly["precipitation"],
        "wind_speed": hourly["wind_speed_10m"],
    })

    df["city"] = location["city"]
    df["state"] = location.get("state")
    df["country"] = location.get("country")
    df["latitude"] = location["latitude"]
    df["longitude"] = location["longitude"]

    return df


def transform_current_weather(weather_json: Dict, location: Dict) -> pd.DataFrame:
    current = weather_json.get("current")
    if not current:
        return pd.DataFrame()

    row = {
        "current_time": current.get("time"),
        "temperature": current.get("temperature_2m"),
        "humidity": current.get("relative_humidity_2m"),
        "precipitation": current.get("precipitation"),
        "wind_speed": current.get("wind_speed_10m"),
        "city": location["city"],
        "state": location.get("state"),
        "country": location.get("country"),
        "latitude": location["latitude"],
        "longitude": location["longitude"],
    }

    return pd.DataFrame([row])


if __name__ == "__main__":
    import json

    with open("sample_weather.json", "r") as f:
        weather_data = json.load(f)

    sample_location = {
        "city": "Bhubaneswar",
        "country": "India",
        "latitude": 20.27241,
        "longitude": 85.83385,
    }

    df = transform_hourly_weather(weather_data, sample_location)
    print(df.head())