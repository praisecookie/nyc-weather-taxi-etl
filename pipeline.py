from dotenv import load_dotenv
load_dotenv() # Load environment variables from .env file

from prefect import flow, task
from src.extract import fetch_weather
from src.transform import clean_weather_data
from src.load import load_weather_to_db

@task(retries=3, retry_delay_seconds=60)
def task_extract():
    return fetch_weather()

@task
def task_transform(raw_data):
    return clean_weather_data(raw_data)

@task
def task_load(clean_df):
    load_weather_to_db(clean_df)

@flow(name="NYC Weather Ingestion Pipeline")
def run_prefect_etl():
    print("--- Starting Enterprise ETL Pipeline ---")
    
    raw_data = task_extract()
    clean_df = task_transform(raw_data)
    task_load(clean_df)
    
    print("--- Pipeline Complete! ---")

if __name__ == "__main__":
    run_prefect_etl()