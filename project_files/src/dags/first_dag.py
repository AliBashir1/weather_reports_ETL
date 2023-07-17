from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor

default_args = {
    "owner": "Ali Bashir",
    "start_date": days_ago(1)
}
# check if database is alive
# check if api credential are working and alive
# start
with DAG(dag_id="first_dag", schedule="@daily", catchup=True, default_args=default_args) as dag:
    start_staging_data = EmptyOperator(task_id="start_staging_data")
    is_data_available = FileSensor(task_id="is_data_available", fs_conn_id="weather_fs_conn", filepath="zipcodes.csv",
                                  timeout=10)

    start_staging_data >> is_data_available
