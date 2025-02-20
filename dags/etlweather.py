from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

# Minsk coordinates
LATITUDE = '53.893009'
LONGITUDE = '27.567444'
# Credentials
POSTGRES_CONNECTION_ID = 'postgres_default'
API_CONNECTION_ID = 'open_meteo_api'

default_args ={
    'owner':'airflow',
    'start_date':days_ago(1)
}

# DAG   
with DAG(dag_id='weather_etl_pipeline', 
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dags:
        
        @task()
        def extract_weather_data():
            """ Extracts weather data from Open-Meteo API using Airflow connection """

            # Using HttpHook to get connection details from Airflow connection

            http_hook = HttpHook(method='GET', http_conn_id=API_CONNECTION_ID)

            # API endpoint
            # URL for Minsk: https://api.open-meteo.com/v1/forecast?latitude=53.893009&longitude=27.567444&current_weather=true
            endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

            # Make the request using Http Hook
            response=http_hook.run(endpoint)

            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f'Failed to fetch weather data: {response.status_code}')
        

        @task()
        def transform_weather_data(weather_data):
            """ Transforms the extracted weather data """

            current_weather = weather_data['current_weather']
            transformed_data = {
                'latitude': LATITUDE,
                'longitude': LONGITUDE,
                'temperature': current_weather['temperature'],
                'windspeed': current_weather['windspeed'],
                'winddirection': current_weather['winddirection'],
                'weathercode': current_weather['weathercode']
            }
            return transformed_data
        

        @task()
        def load_weather_data(transformed_data):
            """ Load the transformed data to PostgreSQL database """
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            # Creating a table if it does not exist using cursor
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP   
            );
        """) 
            
            # Inserting transformed data into the table
            cursor.execute("""
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s) """, (
                transformed_data['latitude'],
                transformed_data['longitude'],
                transformed_data['temperature'],
                transformed_data['windspeed'],
                transformed_data['winddirection'],
                transformed_data['weathercode']
                
            )
            )

            connection.commit()
            cursor.close()


        # Full DAG Workflow ETL Pipeline
        weather_data = extract_weather_data()
        transformed_data = transform_weather_data(weather_data)
        load_weather_data(transformed_data)
