# system
from __future__ import annotations
import datetime as dt
import pendulum

# Airflow
from airflow import DAG
# operators
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# sensors
from airflow.sensors.filesystem import FileSensor
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.http.sensors.http import HttpSensor

# utils
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# ETL
from src.etl_processes.fetch_data import fetch_weather_reports
from src.etl_processes.fetch_data.create_zipcodes_files import create_most_populated_zipcodes_file
from src.utilities.connections.api_connection import get_weather_api_credential



default_args = {
    "owner": "Ali Bashir",
    "start_date": dt.datetime(2023, 7, 24, 0, 0, 0, tzinfo=pendulum.timezone("America/New_York")),
    "email_on_retry": True,
    "email_on_failure": True,
    "email": 'malibashir123123@gmail.com'
}
desc = """
   This DAG shall fetch zip codes from the weather_db database and make an individual call to the www.weatherapi.com 
to fetch data in JSON format, which shall store in the staging directory. 
This process shall run every 6 hours, starting from midnight every day.
"""


with DAG (
        dag_id="fetch_load_hourly_weather_reports",
        description=desc,
        schedule_interval="0 0,6,12,18 * * *",  # 12 am, 6 am, 12 pm, 6 am
        catchup=False,
        default_args=default_args
) as dag:
    # empty operators
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    with TaskGroup(group_id="preliminary_check", default_args=default_args) as preliminary_check:
        is_weather_db_available = SqlSensor(
            task_id="is_weather_db_available",
            conn_id="weather_db_conn",
            sql="""
                SELECT
                    datname
                FROM pg_catalog.pg_database
                    WHERE datname ='weather_db'; """,
            fail_on_empty=True,
            mode="reschedule",
            retries=2,
            retry_delay=10,
            timeout=30,
            trigger_rule=TriggerRule.ALL_FAILED

        )
        is_weather_api_available = HttpSensor(
            task_id="is_weather_api_available",
            http_conn_id="weather_api_conn",
            endpoint="current.json",
            method="GET",
            headers=get_weather_api_credential(),
            request_params={"q": 10314},
            response_check=lambda response: "location" in response.json(),
            mode="reschedule",
            retries=2,
            retry_delay=10,
            timeout=30

        )
        #

        create_zipcodes_file = PythonOperator(
            task_id="create_zipcodes_file",
            python_callable=create_most_populated_zipcodes_file,

        )


    # dag level
    # fetch_weather_reports_api = PythonOperator(
    #     task_id="fetch_weather_reports_api",
    #     python_callable=fetch_weather_reports,
    #     trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    #
    # )
    trig_send_job_reports_dag = TriggerDagRunOperator(
        task_id="trig_send_job_reports_dag",
        trigger_dag_id="send_job_reports_dag",
        wait_for_downstream=False,
        wait_for_completion=False,
        conf={"run_id": "{{ run_id }}",
              "dag_id": "{{ dag.dag_id }}",
              "scheduled_datetime": "{{ data_interval_start }}",
              "execution_date":"{{ logical_date }}",
              "next_scheduled_dagrun_datetime": "{{ data_interval_end }}",
              # params to be used in email and job's table
              "successful_zipcodes_count": "{{ ti.xcom_pull(task_ids='fetch_weather_reports_api')['successful_zipcodes_count'] }}",
              "invalid_zipcodes_count": "{{ ti.xcom_pull(task_ids='fetch_weather_reports_api')['invalid_zipcodes_count']  }}",
              "total_zipcodes": "{{ ti.xcom_pull(task_ids='fetch_weather_reports_api')['total_zipcodes'] }}",
              "job_status": "{{ ti.xcom_pull(task_ids='fetch_weather_reports_api')['job_status'] }}"
              }


    )



    start >> preliminary_check   >> trig_send_job_reports_dag >> end
