import requests


def geocode(city_name: str):
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {
        "name": city_name,
        "count": 1,
        "language": "en",
        "format": "json"
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    if not data.get("results"):
        return None

    result = data["results"][0]
    return {
        "city": result.get("name"),
        "country": result.get("country"),
        "state": result.get("admin1"),
        "latitude": result.get("latitude"),
        "longitude": result.get("longitude"),
        "timezone": result.get("timezone"),
    }


if __name__ == "__main__":
    city = input("Enter city name: ").strip()
    location = geocode(city)
    print(location)