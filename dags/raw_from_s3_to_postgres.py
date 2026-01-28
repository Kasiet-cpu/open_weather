from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable

import pendulum
import logging
import duckdb

BUCKET_NAME = Variable.get('bucket_name')
S3_ACCESS_KEY = Variable.get('access_key')
S3_SECRET_KEY = Variable.get('secret_key')
pg_password = Variable.get('pg_password')

SCHEMA = 'ods'
TARGET_TABLE = 'fct_weather_history'

S3_KEY_TEMPLATE = "agro-data/open_meteo/data/{{ds}}.parquet"


@dag(
    dag_id = 'raw_from_s3_to_postgres',
    start_date = pendulum.datetime(2021, 1, 1, tz="Asia/Almaty"),
    catchup = True,
    schedule = '0 5 * * *',
    max_active_runs = 10,
    tags= ['s3, ods, pg'],
    default_args = {
        'owner': 'open_meteo',
        'retries': 3,
        'retry_delay': pendulum.duration(minutes=5)
    }
)

def load_data_to_pg():
    wait_for_extract_to_s3= S3KeySensor(
        task_id = 'wait_for_extract_to_s3',
        bucket_name = BUCKET_NAME,
        bucket_key = S3_KEY_TEMPLATE,
        poke_interval = 60,
        mode = 'reschedule',
        timeout = 3600,
        aws_conn_id = 'minio_conn'
    )

    @task
    def get_and_transfer_raw_data_to_ods_pg(ds=None):

        con = duckdb.connect()

        con.execute(
            """INSTALL HTTPFS; LOAD HTTPFS; INSTALL postgres; LOAD postgres"""
        )

        con.execute(f"""
            SET s3_endpoint = 'minio:9000';
            SET s3_url_style = 'path';
            SET s3_access_key_id = '{S3_ACCESS_KEY}';
            SET s3_secret_access_key = '{S3_SECRET_KEY}';
            SET s3_use_ssl = False;
    """
        )

        con.execute(f"""
        CREATE SECRET postgres_dwh(
        TYPE postgres,
        HOST 'postgres_dwh',
        PORT 5432,
        DATABASE postgres,
        USER 'postgres',
        PASSWORD '{pg_password}'
        );
        ATTACH '' as postgres_dwh_db (TYPE postgres, SECRET postgres_dwh);
        """)

        con.execute(f"""
            INSERT INTO postgres_dwh_db.{SCHEMA}.{TARGET_TABLE}(
                latitude,
                longitude,
                timestamp,
                temperature_2m,
                relative_humidity_2m,
                apparent_temperature,
                precipitation,
                rain,
                snowfall,
                weather_code,
                surface_pressure,
                cloud_cover,
                shortwave_radiation,
                wind_speed_10m,
                wind_speed_100m,
                wind_direction_10m,
                wind_gusts_10m,
                et0_fao_evapotranspiration,
                vapour_pressure_deficit,
                soil_temperature_0_to_7cm,
                soil_temperature_7_to_28cm,
                soil_temperature_28_to_100cm,
                soil_temperature_100_to_255cm,
                soil_moisture_0_to_7cm,
                soil_moisture_7_to_28cm,
                soil_moisture_28_to_100cm,
                soil_moisture_100_to_255cm,
                load_date
                )
            SELECT
                latitude,
                longitude,
                UNNEST(hourly.time) as timestamp,
                UNNEST(hourly.temperature_2m),
                UNNEST(hourly.relative_humidity_2m),
                UNNEST(hourly.apparent_temperature),
                UNNEST(hourly.precipitation),
                UNNEST(hourly.rain),
                UNNEST(hourly.snowfall),
                UNNEST(hourly.weather_code),
                UNNEST(hourly.surface_pressure),
                UNNEST(hourly.cloud_cover),
                UNNEST(hourly.shortwave_radiation),
                UNNEST(hourly.wind_speed_10m),
                UNNEST(hourly.wind_speed_100m),
                UNNEST(hourly.wind_direction_10m),
                UNNEST(hourly.wind_gusts_10m),
                UNNEST(hourly.et0_fao_evapotranspiration),
                UNNEST(hourly.vapour_pressure_deficit),
                UNNEST(hourly.soil_temperature_0_to_7cm),
                UNNEST(hourly.soil_temperature_7_to_28cm),
                UNNEST(hourly.soil_temperature_28_to_100cm),
                UNNEST(hourly.soil_temperature_100_to_255cm),
                UNNEST(hourly.soil_moisture_0_to_7cm),
                UNNEST(hourly.soil_moisture_7_to_28cm),
                UNNEST(hourly.soil_moisture_28_to_100cm),
                UNNEST(hourly.soil_moisture_100_to_255cm),
                NOW() as load_date
            FROM read_parquet('s3://{BUCKET_NAME}/agro-data/open_meteo/data/{ds}.parquet')
        """
        )
        con.close()
        logging.info("Данные успешно загрузились в Базу данных")

    wait_for_extract_to_s3 >> get_and_transfer_raw_data_to_ods_pg()

load_data_to_pg()