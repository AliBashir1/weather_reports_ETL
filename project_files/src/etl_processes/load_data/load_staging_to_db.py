from __future__ import annotations

import os
import datetime as dt
import pytz
import json
from pandas import DataFrame
from src.utilities.files import WEATHER_REPORT_STAGING_PATH
from src.etl_processes.clean_transform_data.clean_transform_weather_reports import clean_transform_weather_reports
from src.utilities.connections.weather_db_conn import get_weather_db_conn
from sqlalchemy.exc import DBAPIError, SQLAlchemyError, IntegrityError



def get_job_status(files_processed:int = 0, records_processed: int = 0) -> str:
    """
    This function returns a status of daily job. It does it by comparing the number of files and records processed.
    currently expected file number are 4 and expected records are 3122 from each file.
    :param files_processed: a number indicating how many staged file are processed
    :param records_processed: a number indicating how many records combined in all staged files are processed.
    :return:
    """
    expected_number_of_files: int = 4
    expected_number_of_records: int = 3122 * 4  # 12288
    job_status: str = 'Failed'

    if files_processed > 0 and  records_processed > 0:
        if files_processed != expected_number_of_files or records_processed != expected_number_of_records:
            job_status = "Partially Successful"
        elif files_processed == expected_number_of_files and records_processed == expected_number_of_records:
            job_status = "Successful"
    return job_status


def get_staging_reports_path()-> list:
    """
    This function returns a staging file paths.
    staging path format is "../data/staging/2023-08-13/weather-reports-06-PM.json". This method will generate path for
    yesterday files and return it.

    :return: a list of string containing staging file paths.
    """
    staging_files_paths: list = []
    yesterday: dt.date = dt.datetime.today().astimezone(pytz.timezone("America/New_York")).date() - dt.timedelta(days=1)
    # yesterday's staging files name
    for dir, _, files in os.walk(os.path.join(WEATHER_REPORT_STAGING_PATH, str(yesterday) )):
        if len(files) > 0:
            for file in files:
                full_file_path = os.path.join(dir, file)
                staging_files_paths.append(full_file_path)
        else:
            raise ValueError("No File Found")
    return staging_files_paths

def clean_and_load_weather_reports() -> dict:
    """
    This is a main function for daily_weather_reports an airflow job.It gets yesterday staging files by invoking
    "get_staging_reports_path" , clean them by using "clean_transform_weather_reports" function.
    It will gather necessary information like records_processed, files_processed,
    and job status, and it will return it

    RETURNING DATA INFORMATION:
    The returning data is used in airflow job for updating  "daily_job_info" and send status report via email.
    look at "daily_job_info" for further detail.

    :return: a dictionary containing status report of this function.
    """
    staging_weather_reports: list | None = []
    staging_file_paths: list[str] = get_staging_reports_path()
    files_processed: int = len(staging_file_paths)


    # use path to add files
    for staging_file_path in staging_file_paths:
        with open(str(staging_file_path), 'r') as file:
            staging_weather_reports.extend(json.load(file))

    cleaned_weather_reports: DataFrame = clean_transform_weather_reports(staging_weather_reports)
    records_processed = cleaned_weather_reports.shape[0]

    try:
        # load data to database
        with get_weather_db_conn().connect() as con:
            cleaned_weather_reports.to_sql(name="weather_reports_tbl",
                                           if_exists="append",
                                           index=False,
                                           con=con)

        return {"records_processed": records_processed,
                "files_processed": files_processed,
                "job_status": get_job_status(files_processed=files_processed, records_processed=records_processed)
                }

    except (ValueError, DBAPIError, SQLAlchemyError, IntegrityError) as e:
        raise e

if __name__ == "__main__":
    # files = get_staging_reports_path()
    # job_status = get_job_status(len(files), len(files) * 3122)
    # print(job_status)
    files_path = get_staging_reports_path()
    a = clean_transform_weather_reports(files_path)
    print(a)
    # /Users/alibashir/Desktop/workspace.nosync/ETL/weather_reports_project/project_files/src/weather_reports_etl/data/staging/2023-07-29/weather-reports-6-AM.json
    # /Users/alibashir/Desktop/workspace.nosync/ETL/weather_reports_project/project_files/src/weather_reports_etl/data/staging/2023-07-29/weather-reports-6-AM.json