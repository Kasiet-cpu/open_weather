from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import pendulum
import requests
import logging
import json
import os

BUCKET_NAME = Variable.get('bucket_name')
S3_ACCESS_KEY = Variable.get('access_key')
S3_SECRET_KEY = Variable.get('secret_key')


@dag(
    dag_id = "raw_from_open-meteo_to_s3",
    start_date = pendulum.datetime(2021, 1, 1, tz= 'Asia/Almaty'),
    catchup = True,
    max_active_runs = 5,
    schedule = "0 5 * * *",
    default_args = {
        'owner': 'open_meteo',
        'retries': 3,
        'retry_delay': pendulum.duration(minutes=5)
    },
    tags= ['S3, API']
)

def history_data():
    import duckdb
    @task
    def extract_data_from_json_to_parquet(ds=None):
        url = "https://archive-api.open-meteo.com/v1/era5"
        params = {
            'latitude': '51.70',
            'longitude':'71.00',
            'timezone': 'Asia/Almaty',
            'start_date': ds,
            'end_date': ds,
            'hourly': [
                'temperature_2m', 'relative_humidity_2m', 'apparent_temperature',
                'precipitation', 'rain', 'snowfall', 'weather_code', 'surface_pressure',
                'cloud_cover', 'shortwave_radiation', 'wind_speed_10m', 'wind_speed_100m',
                'wind_direction_10m', 'wind_gusts_10m', 'et0_fao_evapotranspiration',
                'vapour_pressure_deficit', 'soil_temperature_0_to_7cm',
                'soil_temperature_7_to_28cm', 'soil_temperature_28_to_100cm',
                'soil_temperature_100_to_255cm', 'soil_moisture_0_to_7cm',
                'soil_moisture_7_to_28cm', 'soil_moisture_28_to_100cm',
                'soil_moisture_100_to_255cm'
            ]
        }
        resp = requests.get(url, params=params, timeout=30, verify= False)
        resp.raise_for_status()

        data = resp.json()

        S3_KEY = f"agro-data/open_meteo/data/{ds}.parquet"
        temp_json_path = f"/tmp/{ds}.json"
        with open(temp_json_path, 'w') as f:
            json.dump(data, f)

        con = duckdb.connect()

        con.execute('''INSTALL HTTPFS; LOAD HTTPFS''')

        con.execute(f"""
        SET s3_endpoint = 'minio:9000';
        SET s3_url_style = 'path';
        SET s3_use_ssl = False;
        SET s3_access_key_id = '{S3_ACCESS_KEY}';
        SET s3_secret_access_key = '{S3_SECRET_KEY}'
        """)

        con.execute(f"""
        COPY(
        SELECT * FROM read_json_auto('{temp_json_path}')
        ) TO 's3://{BUCKET_NAME}/{S3_KEY}' (FORMAT PARQUET)
        """)

        con.close()
        if os.path.exists(temp_json_path):
            os.remove(temp_json_path)
        logging.info(f"Данные успешно загрузились {S3_KEY}")

    extract_data_from_json_to_parquet()

history_data()