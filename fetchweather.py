import requests

HOURLY_VARS = "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m"
CURRENT_VARS = "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m"


def fetch_weather(latitude: float, longitude: float):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": HOURLY_VARS,
        "current": CURRENT_VARS,
        "timezone": "auto",
    }

    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    lat = 20.27241
    lon = 85.83385

    weather_data = fetch_weather(lat, lon)
    print(weather_data.keys())