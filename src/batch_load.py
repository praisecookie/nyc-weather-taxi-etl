import pandas as pd
import duckdb

def load_historical_taxi_data():
    print("1. Spinning up the forklift (Reading Parquet file)...")
    
    # Path to the file you just downloaded
    file_path = "data/raw/yellow_tripdata_2024-01.parquet"
    
    # Read the compressed file into a pandas DataFrame
    df = pd.read_parquet(file_path)
    
    # Let's see how massive this is
    print(f"Original row count: {len(df):,} trips.")
    
    print("\n2. Cleaning the data (The Janitorial Work)...")
    # Real-world data is dirty. Sometimes the meter breaks and records a negative fare.
    # Sometimes GPS glitches and records a 0-mile trip. Let's filter those out.
    clean_df = df[(df['fare_amount'] > 0) & (df['trip_distance'] > 0)]
    
    print(f"Cleaned row count: {len(clean_df):,} trips.")
    removed_rows = len(df) - len(clean_df)
    print(f"We threw out {removed_rows:,} garbage records.")
    
    print("\n3. Loading into the DuckDB Vault...")
    conn = duckdb.connect("data/nyc_warehouse.db")
    
    # DuckDB magic: We can create a table and insert millions of rows from pandas 
    # in one single line of SQL. 
    # We use "CREATE OR REPLACE" so we can run this script multiple times without errors.
    conn.execute("CREATE OR REPLACE TABLE taxi_trips AS SELECT * FROM clean_df")
    
    # Verify the vault
    count = conn.execute("SELECT COUNT(*) FROM taxi_trips").fetchone()[0]
    print(f"\nSuccess! Vault now holds {count:,} historical taxi trips.")
    
    conn.close()

if __name__ == "__main__":
    load_historical_taxi_data()