CREATE DATABASE airflow;
CREATE ROLE airflow_user WITH PASSWORD 'airflowuser123!' LOGIN;
REVOKE connect on DATABASE airflow FROM PUBLIC;
GRANT CONNECT ON DATABASE airflow to airflow_user;
GRANT ALL PRIVILEGES ON DATABASE airflow to airflow_user;