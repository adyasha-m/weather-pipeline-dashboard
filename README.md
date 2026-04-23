# Weather Dashboard

🔗 https://weather-pipeline-dashboard.streamlit.app/

A lightweight end-to-end data pipeline that ingests real-time weather data, processes it into an analytics-ready format, and serves it through a minimal, interactive dashboard.

---

## 🚀 Overview

This project simulates a real-world analytics workflow:

```text
API → Data Pipeline → DuckDB → Streamlit Dashboard
```

It combines:

- **Data ingestion (multi-city, concurrent)**
- **Transformation into structured datasets**
- **Analytics + visualization layer**
- **Clean, minimal UI for exploration**

---

## 📊 Features

### 🔹 Data Pipeline

- Fetches **hourly weather forecast (7 days)** and **current weather**
- Supports **multiple cities in parallel (threaded ingestion)**
- Transforms raw API data into structured tables
- Stores data in **DuckDB (analytics-optimized database)**

---

### 🔹 Analytics Layer

- Weekly forecast metrics (avg, min, max temperature, rainfall)
- Current weather snapshot per city
- Cross-city comparisons
- “Hottest city today” insight
- Aggregations for trends and summaries

---

### 🔹 Dashboard

- Minimal, product-style UI (not typical Streamlit layout)
- KPI cards for key metrics
- Interactive time-series charts
- Heatmap for hourly temperature patterns
- Map view with geo-distribution of cities
- Side-by-side city comparison

---

## 🏗️ Tech Stack

- **Python**
- **DuckDB** (analytical database)
- **Pandas**
- **Plotly** (advanced visualizations)
- **Streamlit** (dashboard)
- **Open-Meteo API** (weather data)

---

## ⚙️ How to Run Locally

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run pipeline (fetch data)

```bash
python pipeline.py
```

### 3. Launch dashboard

```bash
streamlit run dashboard.py
```

---

## 🌐 Deployment

The dashboard is deployed using
👉 Streamlit Community Cloud

### Important:

- The deployed app is **read-only**
- Data is pre-generated and stored in `weather.duckdb`
- Pipeline runs separately (locally or via scheduler)

---

## 📁 Project Structure

```text
weather-pipeline/
│
├── pipeline.py          # Orchestrates ingestion
├── fetchweather.py      # API calls
├── transform.py         # Data transformation
├── load_duckdb.py       # DB writes
├── dashboard.py         # Streamlit app
├── scheduler.py         # Optional automation
├── weather.duckdb       # Database
├── requirements.txt
└── README.md
```

---

## 🧠 Design Decisions

- **DuckDB over SQLite**
  Chosen for better analytical querying and smoother integration with pandas.

- **Threaded ingestion**
  Improves API fetch performance across multiple cities.

- **Separation of concerns**
  Pipeline handles data ingestion; dashboard handles visualization.

- **Minimal UI philosophy**
  Focus on clarity and usability over cluttered dashboards.
