from __future__ import annotations

import datetime as dt
import pytz

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator

from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import SQLExecuteQueryOperator, PostgresOperator

from airflow.utils.task_group import TaskGroup
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule
from src.utilities.helpers import total_execution_time, format_date_filter, print_today_date

default_args = {
    "owner": "Ali Bashir",
    "start_date": dt.datetime(2023,7,26, tzinfo=pytz.timezone("America/New_York") ),
    "email_on_failure":True,
    "email_on_retry": True
}
user_defined_macros = {
    "total_execution_time": total_execution_time,
    "print_today_date": print_today_date
}

user_defined_filters = {
    "format_date_filter": format_date_filter
}

# pushed query results into xcom
def push_start_end_date(context: Context) -> None:
    """Pulls return value of sql ran in fetch_start_end_date and push that
    into xcom with the names start_date and end_date."""
    column_names = ["start_date", "end_date"]
    return_value = context["ti"].xcom_pull(task_ids="fetch_start_end_date")
    for index, value in enumerate(return_value[0]):
        # assign value according to column name.
        key = column_names[index]
        context["ti"].xcom_push(key=key, value=value)

def push_hourly_load_id(context: Context) -> None:
    return_value = context["ti"].xcom_pull(task_ids="six_hourly.fetch_hourly_load_id")
    context["ti"].xcom_push(key="load_id", value= return_value[0][0])

def push_daily_load_id(context: Context) -> None:
    print(context["ti"].task_id)
    return_value = context["ti"].xcom_pull(task_ids="daily_report.fetch_daily_load_id")
    context["ti"].xcom_push(key="load_id", value=return_value[0][0])


def is_it_daily_report(dag_name) -> list | str:
    if dag_name == "fetch_load_daily_weather_reports":
        return ["daily_report.update_daily_job_table"]
    elif dag_name == "fetch_load_hourly_weather_reports":
        return ["six_hourly.update_hourly_jobs_table"]
    else:
        raise ValueError("dag_name cannot be none.")



with DAG(
        dag_id="send_job_reports_dag",
        schedule="@once",
        default_args=default_args,
        template_searchpath="/project_files/src/",
        user_defined_filters=user_defined_filters,
        user_defined_macros=user_defined_macros,

         ) as dag:

    start = EmptyOperator(task_id="Start") # allow previous dag to run and update status in db
    end = EmptyOperator(task_id="End", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    fetch_start_end_date = SQLExecuteQueryOperator(
        task_id="fetch_start_end_date",
        conn_id="airflow_db_conn",
        sql="""
            SELECT
                cast(start_date as varchar(100)),
                cast(end_date as varchar(100))
            FROM dag_run dr
                WHERE run_id = '{{ params.run_id }}'
                    and dag_id = '{{ params.dag_id }}';""",
        on_success_callback=push_start_end_date


    )
    which_reports_to_send = BranchPythonOperator(
        task_id="which_reports_to_send",
        python_callable=is_it_daily_report,
        op_args=["{{ params.dag_id }}"]
    )

    with TaskGroup(group_id="six_hourly") as six_hours_report:

        send_six_hourly_email_report = EmailOperator(
            task_id="send_hourly_email",
            to=["malibashir10@gmail.com"],
            subject="{{ params.dag_id }} job's report",
            html_content="include/html/email/hourly_weather_reports_stats.html"

        )
        fetch_hourly_load_id = SQLExecuteQueryOperator(
            task_id="fetch_hourly_load_id",
             conn_id="weather_db_conn",
             sql="""
             SELECT 
                hourly_load_id 
             FROM hourly_jobs_info_staged
                WHERE job_run_id = '{{ params.run_id }}'; """,
             on_success_callback=push_hourly_load_id
                                                     )

        insert_hourly_jobs_info = PostgresOperator(
            task_id="update_hourly_jobs_table",
            postgres_conn_id="weather_db_conn",
            sql="include/sql/insert_hourly_job_info.sql"

        )

        insert_hourly_jobs_info >> fetch_hourly_load_id >> send_six_hourly_email_report


    with TaskGroup(group_id="daily_report") as daily_report:
        send_daily_email_report = EmailOperator(
            task_id="send_daily_report_email",
            to=["malibashir10@gmail.com"],
            subject="{{ params.dag_id  }} job's report",
            html_content="include/html/email/daily_weather_reports_stats.html"
        )
        fetch_daily_load_id = SQLExecuteQueryOperator(
            task_id="fetch_daily_load_id",
            conn_id="weather_db_conn",
            sql="""
                     SELECT 
                        daily_load_id 
                     FROM daily_jobs_info 
                        WHERE job_run_id = '{{ params.run_id }}'; """,
            on_success_callback=push_daily_load_id ),

        insert_daily_report_info = PostgresOperator(
            task_id="update_daily_job_table",
            postgres_conn_id="weather_db_conn",
            sql="include/sql/insert_daily_job_info.sql"

        )
        insert_daily_report_info >> fetch_daily_load_id  >>send_daily_email_report

    start >> fetch_start_end_date >> which_reports_to_send >> [six_hours_report, daily_report] >> end
