import os
import requests
from dotenv import load_dotenv

load_dotenv()

def fetch_weather():
    """Fetches live weather data from OpenWeatherMap."""
    api_key = os.getenv("OPENWEATHER_API_KEY")
    lat, lon = 40.7128, -74.0060
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json() # We return the data instead of just printing it
    else:
        raise Exception(f"API Error: {response.status_code}")