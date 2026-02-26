import duckdb

def analyze_surge_patterns():
    print("Unlocking the vault to look for patterns...\n")
    
    # 1. Connect to our database
    conn = duckdb.connect("data/nyc_warehouse.db")
    
    # 2. Write our SQL Query
    # We are extracting the 'hour' from the pickup time, counting the trips, 
    # and calculating the average fare. Then we group it by the hour and sort it!
    query = """
        SELECT 
            EXTRACT(hour FROM tpep_pickup_datetime) AS hour_of_day,
            COUNT(*) AS total_trips,
            ROUND(AVG(fare_amount), 2) AS avg_fare
        FROM taxi_trips
        GROUP BY hour_of_day
        ORDER BY total_trips DESC
        LIMIT 5;
    """
    
    # 3. Execute the query and turn it straight into a pandas DataFrame for a pretty print
    print("Running SQL query across 2.8 million rows...")
    result_df = conn.execute(query).df()
    
    print("\n--- TOP 5 BUSIEST TAXI HOURS IN NYC (Jan 2024) ---")
    print(result_df.to_string(index=False))
    print("--------------------------------------------------\n")
    
    # 4. Lock the vault
    conn.close()

if __name__ == "__main__":
    analyze_surge_patterns()