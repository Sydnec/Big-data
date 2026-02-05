from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
from pymongo import MongoClient
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'get_api',
    default_args=default_args,
    description='Fetch weather data from Open Meteo API and store in MongoDB',
    schedule_interval=timedelta(seconds=10),
    catchup=False,
    tags=['weather', 'api', 'occitanie'],
)


def fetch_and_store_weather(**context):
    config_path = '/opt/airflow/dags/config/occitanie_cities.json'
    with open(config_path) as f:
        cities = json.load(f)

    print(f"Loaded {len(cities)} cities from configuration")

    mongo_user = os.getenv('MONGO_INITDB_ROOT_USERNAME', 'admin')
    mongo_pass = os.getenv('MONGO_INITDB_ROOT_PASSWORD', 'admin')
    mongo_host = os.getenv('MONGO_HOST', 'mongodb')
    mongo_port = os.getenv('MONGO_PORT', '27017')

    connection_string = f'mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/'

    print(f"Connecting to MongoDB at {mongo_host}:{mongo_port}...")
    mongo_client = MongoClient(connection_string)

    db = mongo_client['weather']
    collection = db['raw_weather']

    weather_params = [
        'temperature_2m',
        'precipitation',
        'wind_speed_10m',
        'wind_direction_10m',
        'relative_humidity_2m',
        'pressure_msl',
        'soil_temperature_0_to_7cm',
        'soil_moisture_0_to_7cm',
        'cloud_cover'
    ]

    success_count = 0
    error_count = 0

    for city_info in cities:
        city = city_info['city']
        lat = city_info['latitude']
        lon = city_info['longitude']

        print(f"\nProcessing {city} (lat: {lat}, lon: {lon})...")

        try:
            url = 'https://api.open-meteo.com/v1/forecast'
            params = {
                'latitude': lat,
                'longitude': lon,
                'hourly': ','.join(weather_params),
                'timezone': 'Europe/Paris'
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            print(f"API response received for {city}")

            document = {
                'city': city,
                'latitude': lat,
                'longitude': lon,
                'timestamp': datetime.now(),
                'ingestion_timestamp': datetime.now(),
                'execution_date': context['ds'],
                'dag_run_id': context['run_id'],
                'data': data
            }

            result = collection.insert_one(document)
            print(f"Data stored in MongoDB for {city} (doc_id: {result.inserted_id})")
            success_count += 1

        except requests.exceptions.RequestException as e:
            print(f"API error for {city}: {str(e)}")
            error_count += 1
        except Exception as e:
            print(f"Error processing {city}: {str(e)}")
            error_count += 1

    mongo_client.close()

    print(f"Success: {success_count}/{len(cities)} cities")
    print(f"Errors: {error_count}/{len(cities)} cities")

    if success_count == 0:
        raise Exception("Failed to collect data for any city")

    return {
        'success_count': success_count,
        'error_count': error_count,
        'total_cities': len(cities)
    }


fetch_task = PythonOperator(
    task_id='fetch_and_store_weather',
    python_callable=fetch_and_store_weather,
    dag=dag,
    provide_context=True,
)
