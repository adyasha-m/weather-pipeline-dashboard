from concurrent.futures import ThreadPoolExecutor, as_completed

from geocoding import geocode
from fetchweather import fetch_weather
from transform import transform_hourly_weather, transform_current_weather
from load import upsert_city_weather


def process_city(city: str):
    print(f"\n🚀 Start: {city}")

    location = geocode(city)
    if not location:
        print(f"❌ {city}: not found")
        return None

    print(f"📍 {city}: got coordinates")

    weather_data = fetch_weather(location["latitude"], location["longitude"])
    print(f"🌦️ {city}: weather fetched")

    hourly_df = transform_hourly_weather(weather_data, location)
    current_df = transform_current_weather(weather_data, location)

    print(f"🔄 {city}: transformed {len(hourly_df)} hourly rows")
    return city, hourly_df, current_df


def run_pipeline_for_cities(cities):
    results = []

    with ThreadPoolExecutor(max_workers=min(8, len(cities))) as executor:
        futures = {executor.submit(process_city, city): city for city in cities}

        for future in as_completed(futures):
            city = futures[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                print(f"❌ Error in {city}: {e}")

    print("\n💾 Writing to DuckDB...\n")
    for city, hourly_df, current_df in results:
        try:
            upsert_city_weather(hourly_df, current_df)
            print(f"💾 Done: {city}")
        except Exception as e:
            print(f"❌ DB error in {city}: {e}")


if __name__ == "__main__":
    cities = ["Bhubaneswar", "Delhi", "Mumbai", "Bengaluru"]
    run_pipeline_for_cities(cities)