from fastapi import FastAPI
import duckdb
from datetime import datetime

# 1. Turn on the "Open" sign (Initialize the API)
app = FastAPI(title="🚕 NYC Taxi Surge Engine API")

# 2. Define our Endpoint (The Drive-Thru Menu Item)
# The mobile app will hit the URL ending in "/surge-price"
@app.get("/surge-price")
def calculate_surge(base_fare: float = 10.00):
    """
    Calculates the real-time surge multiplier based on live weather and rush hour.
    """
    conn = duckdb.connect("data/nyc_warehouse.db")
    
    # Check current time
    current_hour = datetime.now().hour
    
    # Grab the LATEST weather record from your Robot Butler's hard work
    weather_query = "SELECT condition, temperature_c FROM live_weather ORDER BY timestamp DESC LIMIT 1"
    latest_weather = conn.execute(weather_query).fetchone()
    conn.close()
    
    # Fallbacks in case the weather database is empty
    condition = latest_weather[0] if latest_weather else "Clear"
    temp = latest_weather[1] if latest_weather else 20.0
    
    # The Business Logic (The Brain)
    multiplier = 1.0
    reasons = []
    
    if 15 <= current_hour <= 19:
        multiplier += 0.4
        reasons.append("Evening Rush Hour (+40%)")
        
    if condition in ["Rain", "Snow", "Thunderstorm", "Drizzle"]:
        multiplier += 0.5
        reasons.append("Severe Weather (+50%)")
    elif condition in ["Clouds", "Mist"]:
        multiplier += 0.1
        reasons.append("Cloudy Conditions (+10%)")
        
    if temp < 5.0:
        multiplier += 0.2
        reasons.append("Freezing Temperatures (+20%)")
        
    final_price = round(base_fare * multiplier, 2)
    
    # 3. Hand the bag out the window (Return pure JSON data)
    return {
        "timestamp": datetime.now().isoformat(),
        "current_weather": condition,
        "temperature_c": temp,
        "base_fare": base_fare,
        "surge_multiplier": round(multiplier, 2),
        "final_price": final_price,
        "surge_reasons": reasons
    }