from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.http.sensors.http import HttpSensor





default_args ={
    "owner": "Ali Bashir",
    "start_date": days_ago(1)
}
# check if database is alive
# check if api credential are working and alive
# start
with DAG(dag_id="first_dag", schedule="@daily", catchup=True, default_args=default_args)as dag:
    start_staging_data = EmptyOperator(task_id="start_staging_data")

    is_weather_db_available = SqlSensor(
        task_id="is_weather_db_available",
        conn_id="weather_db_conn",
        sql="select tablename from pg_catalog.pg_tables where schemaname ='public' and tablename = 'zipcodes_tbl';",
        fail_on_empty=True

    )
    is_weather_api_available = HttpSensor(
        task_id="is_weather_api_available",
        http_conn_id="weather_api_conn",
        request_params={"q": 10314},
        endpoint="current.json",
        headers={
            "x-rapidapi-host": "weatherapi-com.p.rapidapi.com",
            "x-rapidapi-key": "41a9feeb31msh474614186513ee4p1772adjsn77dfd8724213"
        },
        response_check= lambda response: "location" in response.json()

    )

    start_staging_data >> [is_weather_db_available, is_weather_api_available]