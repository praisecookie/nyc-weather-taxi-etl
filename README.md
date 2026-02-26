# 🚕 NYC Taxi Surge Pricing ETL & Dynamic Pricing Engine

## 📖 Professional Summary

The purpose of this project is to solve a core business problem for ride-hailing applications: **Dynamic Surge Pricing**.

Historically, surge pricing models fail during sudden environmental changes because their data ingestion pipelines are too slow. This project builds an automated, fault-tolerant ETL (Extract, Transform, Load) pipeline that bridges the gap between historical traffic patterns and real-time environmental factors. By combining millions of historical NYC Yellow Taxi trips with live, automated weather data ingestion, this system feeds a dynamic pricing engine capable of instantly calculating ride multipliers based on real-time conditions (e.g., rainstorms) and historical rush-hour congestion.

The result is a robust backend ecosystem that maximizes driver availability during high-demand windows and optimizes company revenue.

## 🏗️ Architecture & Tech Stack

This project is built using a modern, lightweight, and highly performant data stack.

- **Python (3.10+):** The core programming language for extraction, transformation, and business logic.
- **Prefect:** Enterprise-grade orchestration. Used to build fault-tolerant workflows, manage API rate limits, and automate the 15-minute live data ingestion intervals with built-in retry logic.
- **DuckDB:** An ultra-fast, in-process analytical SQL database. Used as the central data warehouse to join massive historical batch data with real-time micro-batches.
- **Pandas & PyArrow:** Used for heavy-duty data transformation, cleaning, and reading highly compressed columnar data (`.parquet`).
- **OpenWeatherMap API:** External live data source for real-time environmental context.
- **FastAPI & Uvicorn:** The lightning-fast web framework and server used to deploy the pricing engine as a production-ready REST API for software engineering teams.
- **Streamlit:** Used to rapidly prototype the interactive Business Intelligence (BI) dashboard and pricing simulator for stakeholders.
- **Plotly:** The interactive graphing library used to visualize historical surge patterns and ride volumes.

## ⚙️ Key Features

1. **Micro-Batch API Ingestion:** An orchestrated Prefect flow that pings a live weather API, cleans the JSON payload, and appends it to the database to maintain a real-time view of city conditions.
2. **Big Data Batch Loading:** A scalable ingestion script capable of processing and cleaning millions of historical trip records using Parquet files and DuckDB in seconds.
3. **Data Analytics & Aggregation:** SQL-based analytics to identify baseline surge patterns (e.g., top 5 busiest hours of the day and their impact on average fares).
4. **Dynamic Pricing Engine:** A simulated backend endpoint that calculates instant fare multipliers based on combined rulesets (current time vs. historical rush hours + live weather conditions).
5. **Interactive Executive Dashboard:** A Streamlit-powered BI tool featuring Plotly visualizations of 2.8 million rides to help Product Owners identify historical rush hour patterns.
6. **Production REST API:** A FastAPI endpoint (with auto-generated Swagger UI documentation) that accepts ride requests and returns live, JSON-formatted surge pricing payloads for mobile app integration.

## 📂 Project Structure

```
nyc-weather-taxi-etl/
│
├── data/
│ ├── raw/                  # Compressed Parquet files (NYC TLC data)
│ └── nyc_warehouse.db      # The live DuckDB analytical database
│
├── src/
│ ├── api.py                # FastAPI drive-thru window endpoint
│ ├── extract.py            # API connection and data retrieval
│ ├── transform.py          # Pandas data cleaning and formatting
│ ├── load.py               # Database insertion logic
│ ├── batch_load.py         # Heavy-lifting script for millions of historical rows
│ ├── analyze.py            # SQL aggregations and pattern discovery
│ └── pricing_engine.py     # The final business-logic endpoint
│
├── app.py                  #Streamlit executive dashboard & simulator
├── pipeline.py             # The main Prefect orchestrator flow
├── .env                    # Hidden environment variables (API Keys)
├── .gitignore              # Security and version control rules
└── README.md               # Project documentation
```

## 🚀 Local Setup & Execution

**1. Clone the repository and set up the environment:**

```bash
git clone https://github.com/YOUR_USERNAME/nyc-weather-taxi-etl.git
cd nyc-weather-taxi-etl
python3 -m venv venv
source venv/bin/activate
pip install pandas pyarrow duckdb prefect requests python-dotenv fastapi uvicorn streamlit plotly
```

**2. Configure Secrets:**
Create a \`.env\` file in the root directory and add your OpenWeatherMap API key:

```text
OPENWEATHER_API_KEY=your_api_key_here
```

**3. Run the live Prefect pipeline:**

```bash
python pipeline.py
```

**4. Execute the Pricing Engine:**

```bash
python src/pricing_engine.py
```

**5. Launch the Executive Dashboard (Streamlit):**

```bash
streamlit run app.py
```

**6. Start the Production API Server (FastAPI):**

```bash
uvicorn src.api:app --reload
```

_(Then visit http://127.0.0.1:8000/docs to test the API via Swagger UI)_

## 👤 Author

**Praise Cookie Lou** | Data Scientist & Technical Operations Officer
