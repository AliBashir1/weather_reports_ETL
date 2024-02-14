
# Weather Reports ETL

This project serves as a demonstration of proficiency in various technologies highlighted in the resume of the creator. It showcases the application of advanced skills in technologies such as Apache Airflow, pandas, database modeling, SQL scripting, shell scripting, and Docker. The Weather Reports ETL project specifically aims to establish a comprehensive weather data repository for the most populated zip codes in the United States, facilitating easy access for analysis.

## Goal

The primary objective of the Weather Reports ETL project is to automate the collection and storage of weather data for analysis purposes. By implementing an automated pipeline, the project aims to provide accurate insights into weather patterns and empower data-driven decision-making. The project utilizes Apache Airflow to orchestrate the ETL (Extract, Transform, Load) process, with Python scripts handling data extraction, transformation, and loading tasks.

## Scope

To achieve its goal, the project includes the following components and functionalities:

- Automated extraction of 6-hourly weather reports  from weatherapi.com.
- Cleaning and transformation of the extracted data to prepare it for analysis.
- Storage of the cleaned and transformed data in a PostgreSQL database.
- Containerization of Apache Airflow, PostgreSQL database, and Python ETL scripts using Docker.
- Configuration of Apache Airflow server with a Celery Executor worker using Redis.
- Implementation of Docker Compose, Dockerfiles, and entry point shell scripts to configure containers.
- Creation of SQL scripts for setting up necessary databases, tables, procedures, triggers, users, and permissions.
- Development of Apache Airflow DAGs (Directed Acyclic Graphs) for scheduling 6-hourly and daily jobs, as well as sending reports via email.
- Utilization of the Requests library to create custom sessions for making API requests to weatherapi.com.
- Encryption of sensitive information such as API keys, database credentials, and AWS credentials using the Cryptography library.

## Technologies and Programming language Used 
### with examples
I have added a directory path to see an example of work I have done in respective technology.

- Apache Airflow
  	DAG examples:
  		project_files/src/dags/send_job_reports.py
  		project_files/src/dags/hourly_weather_reports.py
  project_files/src/dags/daily_weather_reports.py

	
- Docker Compose
	  weather_reports_compose.yml at root 
- Docker file
	  Docker/airflow/Dockerfile
- Postgres 
	  Docker/weather_db/scripts/sql/initdb/setup_weather_db.sql
		
- Redis
	  It is part of the docker compose file.
- Python
  	Everything is pretty much in Python here is my pick. 
  	project_files/src/dags/hourly_weather_reports.py
    project_files/src/weather_reports_etl/connections/api_connection.py
    project_files/src/weather_reports_etl/etl_processes/clean_transform_data
      /clean_transform_weather_reports.py
	
		
- AWS S3
	  project_files/src/plugins/sensors/S3BucketSensor.py

## Project Structure
weather_reports_etl/
----Docker/
	    This directory has everything related to docker.
--------weather_db/ 
	          All resources and scripts needed to set up the weather database container.			
--------airflow/
	          All the resources and scripts needed to set up the Apache Airflow server includes 
             Redis as msg broker, airflow celery worker, airflow webserver, and scheduler.
----project_files/
      This module is the root of the project. It has everything that is needed to run a project.
--------src/ 
	          Source Code of the project
------------dags/ 
                This module is the central hub for DAGs responsible for executing ETL processes and dispatching email reports.
------------plugins/
		            This module has a sensor for the AWS s3 bucker. 
------------include/
                This module encompasses the HTML markup for email reports and SQL scripts tailored to store DAG job metrics, 
                including execution time and other relevant data.
------------utilities/
		            This module holds utilities for the airflow dags.
------------weather_reports_etl/
		            This is the main ETL module.
----------------etl_processes/
                    The module is dedicated to ETL processes, which encompass tasks such as data retrieval from weatherapi.com or PostgreSQL databases. 
                    It includes data cleaning and transformation operations, as well as loading data into staging or loading data to the PostgreSQL databases.	
----------------utilities/
                    This module houses utilities such as config parsers, encryption tools, and file-related functions like file paths 
                    and a logger instance to the application process.
----------------connections/
                    This module contains a connection Python script to AWS  s3, Postgres database, and api_connection to weatherapi.com.
		
![image](https://github.com/AliBashir1/weather_reports_ETL/assets/31482997/08eaa3fe-4e03-4f05-b55d-b18ad6f891f6)
