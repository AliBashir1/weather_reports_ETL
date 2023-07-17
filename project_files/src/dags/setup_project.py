# check db,tables, data
# create db, tables, data

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "Ali Bashir",
    "start_date": days_ago(2),
    "email_on_retry": True,
    "email_on_failure": True

}


with DAG(dag_id="setup_weather_db", schedule_interval="@once", catchup=False, default_args=default_args ) as dag:
    does_weather_db_exists = SqlSensor(
        task_id="does_weather_db_exists",
        conn_id="weather_db_conn",
        sql="SELECT datname FROM pg_catalog.pg_database WHERE  datname = 'weather_db';",
        fail_on_empty=True


    )
    does_zipcode_tbl_exists = SqlSensor(
        task_id="does_zipcode_tbl_exists",
        conn_id="weather_db_conn",
        sql="SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname ='public' AND tablename = 'zipcodes_tbl';",
        fail_on_empty=True

    )
    does_weather_reports_exists = SqlSensor(
        task_id="does_weather_reports_exists",
        conn_id="weather_db_conn",
        sql="SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname ='public' AND tablename = 'weather_reports_tbl';",
        fail_on_empty=True
    )

    setup_weather_db = BashOperator(
        task_id="setup_weather_db",
        bash_command="chmod +x /docker-entrypoint-initdb.d/setup_weather_db.sql.sh | /docker-entrypoint-initdb.d/setup_weather_db.sql.sh ",
        trigger_rule="one_failed"
    )


    does_weather_db_exists >> [does_weather_reports_exists, does_zipcode_tbl_exists] >> setup_weather_db