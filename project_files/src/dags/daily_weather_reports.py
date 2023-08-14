import datetime as dt
import pytz
import os

# Airflow
from airflow import DAG

# operators
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# sensor
from airflow.sensors.filesystem import FileSensor
from airflow.providers.common.sql.sensors.sql import SqlSensor
# utils
from airflow.utils.task_group import TaskGroup
from airflow.utils.context import Context

# project
from src.weather_reports_etl.etl_processes.load_data.load_staging_to_db import ( get_staging_reports_path,
                                                                                clean_and_load_weather_reports )
default_args = {
    "owner": "Ali Bashir",
    "start_date": dt.datetime(2023,7,29,1, tzinfo=pytz.timezone("America/New_York")),
    "email_on_retry": True,
    "email_on_fail": True
}

description = "write some description for this"


with DAG(dag_id="fetch_load_daily_weather_reports",
         schedule="30 0 * * *", # give hourly job half an hour to process.
         catchup=False,
         default_args=default_args
         )as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup(group_id="preliminary_check") as preliminary_check:
        staging_file_path = get_staging_reports_path()
        # create sensors for each file in staging directory
        for file_path in staging_file_path:
            is_file_available = FileSensor(
                task_id="is_{}_available".format(os.path.basename(file_path)),
                filepath="staging",
                fs_conn_id="weather_fs_conn"
            )

        is_weather_db_available = SqlSensor(
            task_id="is_weather_db_available",
            conn_id="weather_db_conn",
            sql="""
                    SELECT 
                        datname 
                    FROM pg_catalog.pg_database 
                        WHERE datname ='weather_db';
                """,
            fail_on_empty=True,
            mode="reschedule",
            retries=2,
            retry_delay=10,
            timeout=30,
        )
    clean_and_load_weather_staging_reports = PythonOperator(
        task_id="clean_and_load_weather_staging_reports",
        python_callable=clean_and_load_weather_reports
    )

    trig_send_job_reports_dag = TriggerDagRunOperator(
        task_id="trig_send_job_reports_dag",
        trigger_dag_id="send_job_reports_dag",
        wait_for_completion=False,
        wait_for_downstream=False,
        conf={"run_id": "{{ run_id }}",
              "dag_id": "{{ dag.dag_id }}",
              "scheduled_datetime": "{{ data_interval_start }}",
              "execution_date": "{{ logical_date }}",
              "next_scheduled_dagrun_datetime": "{{ data_interval_end }}",
                # params to be used in email and job's table
              "records_processed": "{{ ti.xcom_pull(task_ids='clean_and_load_weather_staging_reports')['records_processed'] }}",
              "files_processed": "{{ ti.xcom_pull(task_ids='clean_and_load_weather_staging_reports')['files_processed'] }}",
              "job_status": "{{ ti.xcom_pull(task_ids='clean_and_load_weather_staging_reports')['job_status'] }}"
              }
    )

    start >> preliminary_check >>  clean_and_load_weather_staging_reports >> trig_send_job_reports_dag >>end
