from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow import DAG
from datetime import datetime
from src.weather_reports_etl.etl_processes.fetch_data import fetch_data_db, fetch_data_api
from src.weather_reports_etl.etl_processes.fetch_data.fetch_data_db import fetch_most_populated_zipcodes
from src.weather_reports_etl.etl_processes.fetch_data import fetch_data_db

default_args = {
    "owner": "Ali Bashir",
    "start_date": datetime(2023, 6, 15, 0, 0, 0),
    "email_on_retry": True,
    "email_on_failure": True
}
desc = """
    This DAG shall fetch zip codes from the weather_db database and make an individual call to the www.weatherapi.com 
    to fetch data in JSON format, which shall be loaded into the AWS S3 bucket "weather-reports-staging". 
    This process shall run every 6 hours, starting from 12 am (midnight) everyday.
"""

with DAG(dag_id="load_staging_data", description=desc, schedule_interval="0 0,6,12,18 * * *", catchup=False, default_args=default_args) as dag:
    is_zipcodes_file_available = FileSensor(
        task_id="is_zipcodes_file_available",
        fs_conn_id="weather_fs_conn",
        poke_interval=10,
        timeout=22,
        filepath="zipcodes.csv",
        recursive=False
    )
    create_zipcodes_file = PythonOperator(
        task_id='create_zipcodes_file',
        python_callable=fetch_most_populated_zipcodes,
        trigger_rule="all_failed"

    )

    is_zipcodes_file_available >> create_zipcodes_file