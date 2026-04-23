from datetime import datetime, timezone
import duckdb
import pandas as pd

DB_PATH = "weather.duckdb"


def get_connection():
    return duckdb.connect(DB_PATH)


def create_tables(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS weather_forecast (
        timestamp VARCHAR,
        temperature DOUBLE,
        humidity DOUBLE,
        precipitation DOUBLE,
        wind_speed DOUBLE,
        city VARCHAR,
        state VARCHAR,
        country VARCHAR,
        latitude DOUBLE,
        longitude DOUBLE,
        ingested_at TIMESTAMP
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS current_weather_snapshot (
        current_time VARCHAR,
        temperature DOUBLE,
        humidity DOUBLE,
        precipitation DOUBLE,
        wind_speed DOUBLE,
        city VARCHAR,
        state VARCHAR,
        country VARCHAR,
        latitude DOUBLE,
        longitude DOUBLE,
        ingested_at TIMESTAMP
    )
    """)


def upsert_city_weather(hourly_df: pd.DataFrame, current_df: pd.DataFrame | None = None):
    if hourly_df is None or hourly_df.empty:
        return

    con = get_connection()
    create_tables(con)

    ingested_at = datetime.now(timezone.utc)

    hourly_df = hourly_df.copy()
    hourly_df["ingested_at"] = ingested_at

    city = hourly_df["city"].iloc[0]

    con.execute("BEGIN TRANSACTION")

    con.execute("DELETE FROM weather_forecast WHERE city = ?", [city])
    con.register("hourly_df", hourly_df)
    con.execute("""
        INSERT INTO weather_forecast BY NAME
        SELECT * FROM hourly_df
    """)
    con.unregister("hourly_df")

    if current_df is not None and not current_df.empty:
        current_df = current_df.copy()
        current_df["ingested_at"] = ingested_at

        con.execute("DELETE FROM current_weather_snapshot WHERE city = ?", [city])
        con.register("current_df", current_df)
        con.execute("""
            INSERT INTO current_weather_snapshot BY NAME
            SELECT * FROM current_df
        """)
        con.unregister("current_df")

    con.execute("COMMIT")
    con.close()