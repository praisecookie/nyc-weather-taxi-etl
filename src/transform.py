import pandas as pd
from datetime import datetime

def clean_weather_data(raw_json):
    """Takes messy JSON and returns a clean pandas DataFrame."""
    print("Transforming the data...")
    
    # 1. Dig into the nested dictionary to find our targets
    temp = raw_json["main"]["temp"]
    condition = raw_json["weather"][0]["main"]
    
    # 2. The API gives time in "UNIX Epoch" format (e.g., 1708860000)
    # We must convert this into a human-readable datetime
    unix_time = raw_json["dt"]
    readable_time = datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')
    
    # 3. Package our clean data into a simple dictionary
    clean_data = {
        "timestamp": readable_time,
        "temperature_c": temp,
        "condition": condition
    }
    
    # 4. Convert it into a pandas DataFrame (a digital spreadsheet row)
    df = pd.DataFrame([clean_data])
    
    return df