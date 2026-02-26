import duckdb
from datetime import datetime

def calculate_surge_price(base_fare=10.00):
    print("--- 🚕 DYNAMIC PRICING ENGINE 🚕 ---")
    print(f"Customer requested a ride. Base fare: ${base_fare:.2f}\n")
    
    conn = duckdb.connect("data/nyc_warehouse.db")
    
    # 1. Check the current time
    current_hour = datetime.now().hour
    
    # 2. Grab the LATEST weather record from your live pipeline
    # We order by timestamp descending and take the top 1
    weather_query = "SELECT condition, temperature_c FROM live_weather ORDER BY timestamp DESC LIMIT 1"
    latest_weather = conn.execute(weather_query).fetchone()
    
    condition = latest_weather[0] if latest_weather else "Clear"
    temp = latest_weather[1] if latest_weather else 20.0
    
    print(f"📍 Current Conditions: Hour {current_hour}:00 | Weather: {condition} ({temp}°C)")
    
    # 3. Apply our Business Rules (The Brain)
    surge_multiplier = 1.0
    reasons = []
    
    # Rule 1: Rush Hour (From our historical analysis!)
    if 15 <= current_hour <= 19:
        surge_multiplier += 0.4  # Add 40%
        reasons.append("Evening Rush Hour (+40%)")
        
    # Rule 2: Bad Weather
    if condition in ["Rain", "Snow", "Thunderstorm"]:
        surge_multiplier += 0.5  # Add 50%
        reasons.append("Severe Weather (+50%)")
    elif condition == "Clouds":
        surge_multiplier += 0.1  # Add 10%
        reasons.append("Cloudy/Overcast (+10%)")
        
    # Rule 3: Extreme Cold
    if temp < 5.0:
        surge_multiplier += 0.2  # Add 20%
        reasons.append("Freezing Temperatures (+20%)")
        
    # 4. Calculate Final Price
    final_price = base_fare * surge_multiplier
    
    print("\n🧾 SURGE CALCULATION:")
    if not reasons:
        print("- Normal demand. No surge applied.")
    else:
        for reason in reasons:
            print(f"- {reason}")
            
    print(f"\n💵 FINAL RIDE PRICE: ${final_price:.2f} (Multiplier: {surge_multiplier:.1f}x)")
    print("------------------------------------")
    
    conn.close()

if __name__ == "__main__":
    calculate_surge_price()