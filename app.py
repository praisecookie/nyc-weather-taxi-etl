import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
from datetime import datetime

# Page Configuration
st.set_page_config(
    page_title="NYC Taxi Surge System",
    page_icon="🚖",
    layout="wide"
)

# Title and Header
st.title("🚖 NYC Taxi Surge Pricing & Analytics")
st.markdown("""
This app demonstrates a real-time **ETL Pipeline** and **Dynamic Pricing Engine**.
The system ingests live weather data and combines it with historical taxi trends to calculate fare multipliers.
""")

# Create two tabs for our two different audiences
tab1, tab2 = st.tabs(["📈 Executive Dashboard", "⚡ Dynamic Pricing Simulator"])

# --- TAB 1: THE EXECUTIVE DASHBOARD (For the Product Owner) ---
with tab1:
    st.header("Historical Taxi Demand Patterns")
    st.write("Analyzing 2.8 million rides from Jan 2024 to identify surge hours.")
    
    # Connect to the vault
    conn = duckdb.connect("data/nyc_warehouse.db")
    
    # Run the Aggregation Query
    query = """
    SELECT 
        EXTRACT(hour FROM tpep_pickup_datetime) AS hour_of_day,
        COUNT(*) AS total_trips,
        AVG(fare_amount) AS avg_fare
    FROM taxi_trips
    GROUP BY hour_of_day
    ORDER BY hour_of_day
    """
    df_trends = conn.execute(query).df()
    conn.close()

    # Create a nice layout with 2 columns
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Trip Volume by Hour")
        # Use Plotly for interactive charts
        fig_vol = px.bar(df_trends, x="hour_of_day", y="total_trips", 
                         title="Rides per Hour (Rush Hour Peaks)",
                         labels={"hour_of_day": "Hour (24h)", "total_trips": "Total Rides"},
                         color="total_trips", color_continuous_scale="Viridis")
        st.plotly_chart(fig_vol, use_container_width=True)
        
    with col2:
        st.subheader("Average Fare by Hour")
        fig_fare = px.line(df_trends, x="hour_of_day", y="avg_fare", 
                           title="Average Fare Price (Supply/Demand)",
                           markers=True,
                           labels={"hour_of_day": "Hour (24h)", "avg_fare": "Avg Fare ($)"})
        fig_fare.update_traces(line_color='#FF4B4B')
        st.plotly_chart(fig_fare, use_container_width=True)

# --- TAB 2: THE PRICING SIMULATOR (For the App Devs) ---
with tab2:
    st.header("⚡ Live Dynamic Pricing Engine")
    st.write("Simulate a user requesting a ride right now based on live pipeline data.")
    
    # Sidebar inputs
    st.sidebar.header("Simulator Controls")
    base_fare = st.sidebar.number_input("Base Fare ($)", value=10.0, step=1.0)
    
    if st.button("📍 Request Ride Now", type="primary"):
        # Connect to DB to get LATEST weather
        conn = duckdb.connect("data/nyc_warehouse.db")
        weather_query = "SELECT condition, temperature_c, timestamp FROM live_weather ORDER BY timestamp DESC LIMIT 1"
        latest_weather = conn.execute(weather_query).fetchone()
        conn.close()
        
        if latest_weather:
            condition, temp, last_updated = latest_weather
        else:
            condition, temp, last_updated = "Unknown", 20.0, datetime.now()
            
        current_hour = datetime.now().hour
        
        # Display Current Context
        st.info(f"**Live Context:** Time: {current_hour}:00 | Weather: {condition} ({temp}°C) | Last Update: {last_updated}")
        
        # --- PRICING LOGIC (Same as your script) ---
        multiplier = 1.0
        reasons = []
        
        # Rule 1: Rush Hour
        if 15 <= current_hour <= 19:
            multiplier += 0.4
            reasons.append("🚗 Evening Rush Hour (+40%)")
            
        # Rule 2: Weather
        if condition in ["Rain", "Snow", "Thunderstorm", "Drizzle"]:
            multiplier += 0.5
            reasons.append("⛈️ Severe Weather (+50%)")
        elif condition in ["Clouds", "Mist"]:
            multiplier += 0.1
            reasons.append("☁️ Cloudy Conditions (+10%)")
            
        final_price = base_fare * multiplier
        
        # Display Results using Metrics
        c1, c2, c3 = st.columns(3)
        c1.metric("Base Fare", f"${base_fare:.2f}")
        c2.metric("Surge Multiplier", f"{multiplier}x", delta_color="inverse")
        c3.metric("Final Price", f"${final_price:.2f}")
        
        if reasons:
            st.warning("Surge Reasons Applied:")
            for r in reasons:
                st.write(f"- {r}")
        else:
            st.success("✅ Standard Pricing (No Surge)")