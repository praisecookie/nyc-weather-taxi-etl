import schedule
import time
from pipeline import run_etl

def job():
    print("\n=========================================")
    print("[ROBOT BUTLER] Waking up to run ETL...")
    
    try:
        run_etl()
        print("[ROBOT BUTLER] Mission accomplished. Going back to sleep.")
    except Exception as e:
        print(f"[ROBOT BUTLER ALARM] The pipeline failed: {e}")
        
    print("=========================================\n")

# 1. We tell the scheduler to run our job every 15 minutes.
# (For testing purposes, let's actually change this to every 10 SECONDS so you don't have to wait!)
schedule.every(10).seconds.do(job)

print("Starting the Orchestrator. Press Ctrl+C to stop the program.")

# 2. Run it immediately the first time so we don't have to wait for the first timer
job()

# 3. The Infinite Loop
# This keeps the script running forever, checking the clock to see if it's time to work.
while True:
    schedule.run_pending()
    time.sleep(1)