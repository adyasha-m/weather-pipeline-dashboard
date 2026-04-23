import sqlite3

import pandas as pd

DB_NAME = "weather1.db"


def get_data():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql("SELECT * FROM weather_data", conn)
    conn.close()

    if df.empty:
        raise ValueError("weather_data table is empty.")

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date.astype(str)
    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.day_name()
    df["is_rain"] = (df["precipitation"].fillna(0) > 0).astype(int)

    return df


def build_latest_snapshot(df):
    idx = df.sort_values("timestamp").groupby("city")["timestamp"].idxmax()
    latest = df.loc[idx, [
        "city", "country", "latitude", "longitude", "timestamp",
        "temperature", "humidity", "precipitation", "wind_speed"
    ]].copy()
    return latest.sort_values("city").reset_index(drop=True)


def build_daily_metrics(df):
    daily = df.groupby(["city", "date"], as_index=False).agg(
        avg_temperature=("temperature", "mean"),
        min_temperature=("temperature", "min"),
        max_temperature=("temperature", "max"),
        avg_humidity=("humidity", "mean"),
        avg_wind_speed=("wind_speed", "mean"),
        total_precipitation=("precipitation", "sum"),
        rainy_hours=("is_rain", "sum"),
        observations=("timestamp", "count"),
    )

    daily["temp_range"] = daily["max_temperature"] - daily["min_temperature"]
    daily["rain_rate_pct"] = (daily["rainy_hours"] / daily["observations"]) * 100
    return daily


def build_city_summary(df):
    summary = df.groupby("city", as_index=False).agg(
        country=("country", "first"),
        latitude=("latitude", "first"),
        longitude=("longitude", "first"),
        observations=("timestamp", "count"),
        days_observed=("date", "nunique"),
        avg_temperature=("temperature", "mean"),
        min_temperature=("temperature", "min"),
        max_temperature=("temperature", "max"),
        temp_std=("temperature", "std"),
        avg_humidity=("humidity", "mean"),
        avg_wind_speed=("wind_speed", "mean"),
        total_precipitation=("precipitation", "sum"),
        rainy_hours=("is_rain", "sum"),
    )

    summary["temp_range"] = summary["max_temperature"] - summary["min_temperature"]
    summary["rain_rate_pct"] = (summary["rainy_hours"] / summary["observations"]) * 100
    summary["temperature_stability"] = summary["temp_std"].fillna(0)
    return summary.sort_values("avg_temperature", ascending=False).reset_index(drop=True)


def build_hourly_profile(df):
    hourly = df.groupby("hour", as_index=False).agg(
        avg_temperature=("temperature", "mean"),
        avg_humidity=("humidity", "mean"),
        avg_wind_speed=("wind_speed", "mean"),
        avg_precipitation=("precipitation", "mean"),
        rainy_hour_pct=("is_rain", "mean"),
    )

    hourly["rainy_hour_pct"] = hourly["rainy_hour_pct"] * 100
    return hourly


def save_table(df, table_name, conn):
    df.to_sql(table_name, conn, if_exists="replace", index=False)


def build_analytics_layer():
    df = get_data()
    conn = sqlite3.connect(DB_NAME)

    latest = build_latest_snapshot(df)
    daily = build_daily_metrics(df)
    summary = build_city_summary(df)
    hourly = build_hourly_profile(df)

    save_table(latest, "latest_city_snapshot", conn)
    save_table(daily, "city_daily_metrics", conn)
    save_table(summary, "city_summary", conn)
    save_table(hourly, "hourly_profile", conn)

    conn.close()
    return latest, daily, summary, hourly


if __name__ == "__main__":
    latest, daily, summary, hourly = build_analytics_layer()
    print("Analytics layer built successfully.")
    print(summary.head())