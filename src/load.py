import duckdb
import pandas as pd

def load_weather_to_db(clean_df):
    """Loads a clean pandas DataFrame into the DuckDB warehouse."""
    print("Loading data into the vault...")
    
    # 1. Connect to our database file (it will create this file if it doesn't exist)
    conn = duckdb.connect("data/nyc_warehouse.db")
    
    # 2. Create the table structure if this is our very first time running the pipeline
    conn.execute("""
        CREATE TABLE IF NOT EXISTS live_weather (
            timestamp TIMESTAMP,
            temperature_c DOUBLE,
            condition VARCHAR
        )
    """)
    
    # 3. DuckDB is so smart it can read pandas DataFrames directly. 
    # We tell it to append our clean_df into the live_weather table.
    conn.execute("INSERT INTO live_weather SELECT * FROM clean_df")
    
    # 4. Check our work by asking the database how many rows it has now
    count = conn.execute("SELECT COUNT(*) FROM live_weather").fetchone()[0]
    print(f"Success! The vault now contains {count} weather records.")
    
    # 5. Lock the vault
    conn.close()