import duckdb
from pathlib import Path
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# from pipeline import run_pipeline_for_cities

DB_PATH = "weather.duckdb"
CITIES = ["Bhubaneswar", "Delhi", "Mumbai", "Bengaluru"]

st.set_page_config(
    page_title="Weather Dashboard",
    page_icon="◌",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    :root {
        --bg: #0b0f14;
        --panel: rgba(255,255,255,0.03);
        --panel-2: rgba(255,255,255,0.05);
        --border: rgba(255,255,255,0.08);
        --text: #e8eef5;
        --muted: rgba(232,238,245,0.68);
        --accent: #8fb3ff;
    }

    .stApp {
        background: radial-gradient(circle at top left, rgba(88, 126, 255, 0.12), transparent 28%),
                    radial-gradient(circle at top right, rgba(138, 210, 255, 0.08), transparent 22%),
                    var(--bg);
        color: var(--text);
    }

    .block-container {
        padding-top: 1.15rem;
        padding-bottom: 1.5rem;
        max-width: 1440px;
    }

    h1, h2, h3, h4, h5, h6, p, span, label, div {
        color: var(--text);
    }

    .stMetric {
        border: 1px solid var(--border);
        padding: 14px 16px;
        border-radius: 18px;
        background: linear-gradient(180deg, var(--panel), rgba(255,255,255,0.01));
        box-shadow: 0 12px 24px rgba(0,0,0,0.18);
    }

    div[data-testid="stHorizontalBlock"] {
        gap: 0.75rem;
    }

    .card {
        border: 1px solid var(--border);
        border-radius: 20px;
        padding: 1rem 1.1rem;
        background: linear-gradient(180deg, var(--panel), rgba(255,255,255,0.015));
        box-shadow: 0 14px 30px rgba(0,0,0,0.16);
    }

    .card-title {
        font-size: 0.82rem;
        letter-spacing: 0.02em;
        text-transform: uppercase;
        color: var(--muted);
        margin-bottom: 0.4rem;
    }

    .card-value {
        font-size: 1.95rem;
        font-weight: 700;
        line-height: 1.05;
        margin-bottom: 0.15rem;
    }

    .card-subtle {
        color: var(--muted);
        font-size: 0.92rem;
    }

    .divider-soft {
        margin: 1rem 0 1.1rem 0;
        border-top: 1px solid var(--border);
    }

    section[data-testid="stSidebar"] {
        background: rgba(255,255,255,0.02);
        border-right: 1px solid var(--border);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Weather Intelligence")
st.caption("Minimal, refined weather analytics.")


def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)


def db_exists() -> bool:
    return Path(DB_PATH).exists()


def safe_load(query: str) -> pd.DataFrame:
    try:
        con = get_connection()
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame()


def metric_card(title: str, value: str, subtitle: str = ""):
    st.markdown(
        f"""
        <div class="card">
            <div class="card-title">{title}</div>
            <div class="card-value">{value}</div>
            <div class="card-subtle">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# def refresh_weather_data():
#     with st.spinner("Fetching fresh weather data..."):
#         run_pipeline_for_cities(CITIES)
#     st.session_state.last_refresh_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     st.session_state.weather_refreshed = True
#     st.success("Weather data refreshed.")


if "weather_refreshed" not in st.session_state:
    st.session_state.weather_refreshed = False

if "selected_city" not in st.session_state:
    st.session_state.selected_city = CITIES[0]

if "last_refresh_at" not in st.session_state:
    st.session_state.last_refresh_at = None

refresh_col, note_col = st.sidebar.columns([1, 1])
# with refresh_col:
#     if st.button("Refresh data now"):
#         refresh_weather_data()
#         st.rerun()

# with note_col:
#     st.caption("Runs once on first open.")

# if not st.session_state.weather_refreshed:
#     refresh_weather_data()
#     st.rerun()

if not db_exists():
    st.error("Database not found. Run pipeline.py first.")
    st.stop()

forecast_df = safe_load("SELECT * FROM weather_forecast")
current_df = safe_load("SELECT * FROM current_weather_snapshot")

if forecast_df.empty:
    st.error("No forecast data found. Run pipeline.py first.")
    st.stop()

forecast_df["timestamp"] = pd.to_datetime(forecast_df["timestamp"], errors="coerce")
if "ingested_at" in forecast_df.columns:
    forecast_df["ingested_at"] = pd.to_datetime(forecast_df["ingested_at"], errors="coerce")

if not current_df.empty:
    current_df["current_time"] = pd.to_datetime(current_df["current_time"], errors="coerce")
    if "ingested_at" in current_df.columns:
        current_df["ingested_at"] = pd.to_datetime(current_df["ingested_at"], errors="coerce")

cities = sorted(forecast_df["city"].dropna().unique().tolist())
if not cities:
    st.error("No cities available in the database.")
    st.stop()

selected_city = st.sidebar.selectbox("City", cities, key="selected_city")
compare_default = [selected_city] if selected_city else cities[:2]
selected_cities = st.sidebar.multiselect("Compare cities", cities, default=compare_default)
if not selected_cities:
    selected_cities = [selected_city]

forecast_city = forecast_df[forecast_df["city"] == selected_city].copy().sort_values("timestamp")
current_city = (
    current_df[current_df["city"] == selected_city].copy().sort_values("current_time")
    if not current_df.empty
    else pd.DataFrame()
)

forecast_city["date"] = forecast_city["timestamp"].dt.date
forecast_city["hour"] = forecast_city["timestamp"].dt.hour

all_city_latest = (
    forecast_df.sort_values("timestamp")
    .groupby("city", as_index=False)
    .tail(1)
    .copy()
)
all_city_latest["date"] = all_city_latest["timestamp"].dt.date

latest_day = forecast_df["timestamp"].dt.date.max()
today_df = forecast_df[forecast_df["timestamp"].dt.date == latest_day].copy()
hottest_today = None
if not today_df.empty:
    hottest_today = today_df.loc[today_df["temperature"].idxmax()]

left_head, right_head = st.columns([2.2, 1])
with left_head:
    st.subheader(selected_city)

    if not forecast_city.empty and "state" in forecast_city.columns:
        state = forecast_city["state"].iloc[0]
        st.write(state)
    else:
        st.write("—")
        
with right_head:
    if "ingested_at" in forecast_city.columns and not forecast_city["ingested_at"].isna().all():
        last_updated = forecast_city["ingested_at"].max()
        metric_card("Last updated", str(last_updated), "latest DB refresh for this city")
    elif st.session_state.last_refresh_at:
        metric_card("Last updated", st.session_state.last_refresh_at, "dashboard refresh time")

st.markdown('<div class="divider-soft"></div>', unsafe_allow_html=True)

overview_tab, forecast_tab, current_tab, map_tab, compare_tab = st.tabs(
    ["Overview", "Weekly Forecast", "Current Weather", "Map View", "Comparisons"]
)

with overview_tab:
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Avg Temp", f"{forecast_city['temperature'].mean():.1f} °C")
    k2.metric("Max Temp", f"{forecast_city['temperature'].max():.1f} °C")
    k3.metric("Rain Total", f"{forecast_city['precipitation'].sum():.1f} mm")
    k4.metric("Wind Avg", f"{forecast_city['wind_speed'].mean():.1f} km/h")

    st.markdown("### Key insight")
    if hottest_today is not None:
        i1, i2 = st.columns([1, 2])
        with i1:
            metric_card(
                "Hottest today",
                f"{hottest_today['city']}",
                f"{latest_day} • {hottest_today['temperature']:.1f} °C",
            )
        with i2:
            st.markdown(
                f"""
                <div class="card">
                    <div class="card-title">Summary</div>
                    <div class="card-value">{hottest_today['temperature']:.1f} °C</div>
                    <div class="card-subtle">Highest hourly temperature among all tracked cities today.</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown("### City snapshot")
    snapshot_df = all_city_latest[all_city_latest["city"].isin(selected_cities)].copy()
    if not snapshot_df.empty:
        st.dataframe(
            snapshot_df[["city", "country", "timestamp", "temperature", "humidity", "precipitation", "wind_speed"]]
            .sort_values("temperature", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

with forecast_tab:
    st.subheader(f"Weekly forecast — {selected_city}")

    f1, f2, f3, f4 = st.columns(4)
    f1.metric("Avg Temp", f"{forecast_city['temperature'].mean():.1f} °C")
    f2.metric("Min Temp", f"{forecast_city['temperature'].min():.1f} °C")
    f3.metric("Max Temp", f"{forecast_city['temperature'].max():.1f} °C")
    f4.metric("Total Rain", f"{forecast_city['precipitation'].sum():.1f} mm")

    left_plot, right_plot = st.columns([1.5, 1])
    with left_plot:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=forecast_city["timestamp"],
                y=forecast_city["temperature"],
                mode="lines",
                name="Temperature",
                line=dict(width=3),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=forecast_city["timestamp"],
                y=forecast_city["humidity"],
                mode="lines",
                name="Humidity",
                line=dict(width=2, dash="dot"),
            )
        )
        fig.update_layout(
            height=420,
            margin=dict(l=10, r=10, t=30, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h"),
            xaxis_title="Time",
            yaxis_title="Value",
        )
        st.plotly_chart(fig, use_container_width=True)

    with right_plot:
        daily = forecast_city.groupby("date", as_index=False).agg(
            avg_temperature=("temperature", "mean"),
            total_rain=("precipitation", "sum"),
            avg_wind=("wind_speed", "mean"),
        )
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=daily["date"], y=daily["total_rain"], name="Rain"))
        fig2.add_trace(
            go.Scatter(
                x=daily["date"],
                y=daily["avg_temperature"],
                mode="lines+markers",
                name="Avg Temp",
            )
        )
        fig2.update_layout(
            height=420,
            margin=dict(l=10, r=10, t=30, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h"),
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("### Forecast table")
    st.dataframe(
        forecast_city[["timestamp", "temperature", "humidity", "precipitation", "wind_speed"]],
        use_container_width=True,
        hide_index=True,
    )

with current_tab:
    st.subheader(f"Current weather — {selected_city}")

    if current_city.empty:
        st.warning("No current weather snapshot available yet. Run the pipeline again.")
    else:
        row = current_city.iloc[-1]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Temperature", f"{row['temperature']:.1f} °C")
        c2.metric("Humidity", f"{row['humidity']:.1f} %")
        c3.metric("Precipitation", f"{row['precipitation']:.1f} mm")
        c4.metric("Wind Speed", f"{row['wind_speed']:.1f} km/h")

        left_live, right_live = st.columns([1, 1])
        with left_live:
            metric_card(
                "Location",
                row["city"],
                f"{row.get('country', '')} • {row.get('latitude', '')}, {row.get('longitude', '')}",
            )
        with right_live:
            metric_card(
                "Current time",
                str(row["current_time"]),
                f"Last updated: {row.get('ingested_at', '')}",
            )

        st.markdown("### Current snapshot table")
        show_cols = [
            "current_time",
            "temperature",
            "humidity",
            "precipitation",
            "wind_speed",
            "city",
            "country",
            "latitude",
            "longitude",
            "ingested_at",
        ]
        st.dataframe(
            current_city[[c for c in show_cols if c in current_city.columns]],
            use_container_width=True,
            hide_index=True,
        )

with map_tab:
    st.subheader("Map view")

    map_source = all_city_latest.copy()

    if not current_df.empty:
        map_latest_current = (
            current_df.sort_values("current_time")
            .groupby("city", as_index=False)
            .tail(1)
            .copy()
        )

        map_source = map_source.merge(
            map_latest_current[
                ["city", "temperature", "humidity", "precipitation", "wind_speed"]
            ],
            on="city",
            how="left",
            suffixes=("_forecast", "_current"),
        )

    if map_source.empty:
        st.info("No map data available.")
    else:
        map_source = map_source.dropna(subset=["latitude", "longitude"]).copy()

        if "temperature_current" in map_source.columns:
            map_source["temp_used"] = map_source["temperature_current"]
        elif "temperature_forecast" in map_source.columns:
            map_source["temp_used"] = map_source["temperature_forecast"]
        else:
            map_source["temp_used"] = 0

        if "humidity_current" in map_source.columns:
            map_source["humidity_used"] = map_source["humidity_current"]
        elif "humidity_forecast" in map_source.columns:
            map_source["humidity_used"] = map_source["humidity_forecast"]
        else:
            map_source["humidity_used"] = 0

        if "precipitation_current" in map_source.columns:
            map_source["precipitation_used"] = map_source["precipitation_current"]
        elif "precipitation_forecast" in map_source.columns:
            map_source["precipitation_used"] = map_source["precipitation_forecast"]
        else:
            map_source["precipitation_used"] = 0

        if "wind_speed_current" in map_source.columns:
            map_source["wind_speed_used"] = map_source["wind_speed_current"]
        elif "wind_speed_forecast" in map_source.columns:
            map_source["wind_speed_used"] = map_source["wind_speed_forecast"]
        else:
            map_source["wind_speed_used"] = 0

        map_source["size_temp"] = map_source["temp_used"].fillna(0).abs() + 10

        fig_map = px.scatter_mapbox(
            map_source,
            lat="latitude",
            lon="longitude",
            color="temp_used",
            size="size_temp",
            hover_name="city",
            hover_data={
                "country": True,
                "temp_used": ":.1f",
                "humidity_used": ":.1f",
                "precipitation_used": ":.1f",
                "wind_speed_used": ":.1f",
                "latitude": False,
                "longitude": False,
                "size_temp": False,
            },
            zoom=3,
            height=560,
            color_continuous_scale=["#1B263B", "#415A77", "#778DA9", "#E0E1DD", "#FCA311"],
        )
        fig_map.update_layout(
            mapbox_style="carto-darkmatter",
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_map, use_container_width=True)
        st.caption("Bubble size tracks temperature magnitude; color shows temperature intensity.")

with compare_tab:
    st.subheader("City comparisons")

    compare_df = forecast_df[forecast_df["city"].isin(selected_cities)].copy()
    if compare_df.empty:
        st.info("Select at least one city to compare.")
    else:
        c_left, c_right = st.columns(2)
        with c_left:
            avg_temp = compare_df.groupby("city")["temperature"].mean().sort_values(ascending=False)
            fig_temp = px.bar(
                avg_temp.reset_index(),
                x="city",
                y="temperature",
                text_auto=".1f",
                title="Average temperature by city",
            )
            fig_temp.update_layout(height=420, margin=dict(l=10, r=10, t=45, b=10))
            st.plotly_chart(fig_temp, use_container_width=True)

        with c_right:
            rain_total = compare_df.groupby("city")["precipitation"].sum().sort_values(ascending=False)
            fig_rain = px.bar(
                rain_total.reset_index(),
                x="city",
                y="precipitation",
                text_auto=".1f",
                title="Total precipitation by city",
            )
            fig_rain.update_layout(height=420, margin=dict(l=10, r=10, t=45, b=10))
            st.plotly_chart(fig_rain, use_container_width=True)

        st.markdown("### Higher-resolution comparison")
        heat_df = compare_df.copy()
        heat_df["hour"] = heat_df["timestamp"].dt.hour
        pivot = heat_df.pivot_table(index="hour", columns="city", values="temperature", aggfunc="mean")
        if not pivot.empty:
            fig_heat = px.imshow(
                pivot.T,
                aspect="auto",
                color_continuous_scale=[
                    [0.0, "#081120"],
                    [0.35, "#14365f"],
                    [0.65, "#4e7cb7"],
                    [1.0, "#ffd166"],
                ],
                labels=dict(x="Hour", y="City", color="Temp"),
                title="Average hourly temperature heatmap",
            )
            fig_heat.update_layout(height=420, margin=dict(l=10, r=10, t=45, b=10))
            st.plotly_chart(fig_heat, use_container_width=True)

        st.markdown("### Ranked city summary")
        summary = compare_df.groupby("city", as_index=False).agg(
            avg_temperature=("temperature", "mean"),
            min_temperature=("temperature", "min"),
            max_temperature=("temperature", "max"),
            total_rain=("precipitation", "sum"),
            avg_humidity=("humidity", "mean"),
            avg_wind=("wind_speed", "mean"),
        )
        summary["temp_range"] = summary["max_temperature"] - summary["min_temperature"]
        st.dataframe(
            summary.sort_values("avg_temperature", ascending=False).round(2),
            use_container_width=True,
            hide_index=True,
        )

st.markdown('<div class="divider-soft"></div>', unsafe_allow_html=True)
st.caption("The dashboard auto-refreshes once on load and also supports manual refresh from the sidebar.")