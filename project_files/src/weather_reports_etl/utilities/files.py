"""
https://towardsdatascience.com/simple-trick-to-work-with-relative-paths-in-python-c072cdc9acb9
"""
from __future__ import annotations
import os
import datetime as dt
from datetime import datetime
import pytz
import csv
from csv import writer
import json


"""
Paths and Files:
The module contains file paths and functions related to creating files etc. 
"""
ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), "../"))
ZIPCODES_FILEPATH = os.path.realpath(os.path.join(os.path.dirname(__file__), "../data/zipcodes.csv" ))
WEATHER_REPORT_STAGING_PATH = os.path.realpath(os.path.join(os.path.dirname(__file__), "../data/staging/"))
ERROR_REPORT_PATH = os.path.realpath(os.path.join(os.path.dirname(__file__), "../error_reports/"))

def create_file_name(weather_reports: bool = True) -> str:
    """
    This function will produce a path for either the errors or weather reports.
    It will generate a file name that corresponds with the current six-hour interval,
    ensuring consistency throughout the process
    :param weather_reports:
    :return: a file path of string type.
    """
    now: datetime = dt.datetime.now(pytz.timezone("America/New_York"))
    hour: int = now.time().hour
    key_hour: str | None = None
    if 0 <= hour < 6:
        key_hour = "12-AM"
    if 6 <= hour < 12:
        key_hour = "06-AM"
    elif 12 <= hour < 18:
        key_hour = '12-PM'
    elif 18 <= hour <= 24:
        key_hour = "06-PM"
    if weather_reports:
       file_name = f"{now.strftime('%Y-%m-%d')}/weather-reports-{key_hour}.json"
    else:
       file_name =  f"{now.strftime('%Y-%m-%d')}/error-reports-{key_hour}.csv"

    return file_name


def write_error_report(zipcode: str | None = None,
                       http_status_code: str | None = None,
                       description: str | None= None,
                       api_errors: int | None =None,
                       retry: str ="No") -> None:
    """
    This function will generate a daily CSV error file named after the current date and time,
    such as "2023-08-13/errors-reports-6pm.csv".
    :param zipcode: a zipcode of string type
    :param http_status_code: a http status code in string type
    :param description: Description of the error
    :param api_errors: an api error code.
    :param retry: yes or no
    :return: None
    """

    data: list[str] = [zipcode, http_status_code, description, api_errors, retry]
    error_report_file: str = os.path.join(ERROR_REPORT_PATH, create_file_name(weather_reports=False))

    # create parent directories if doesnt exists
    if not os.path.exists(os.path.dirname(error_report_file)):
        os.makedirs(os.path.dirname(error_report_file))

    if os.path.exists(error_report_file):
        mode = 'a'
    else:
        mode = 'w'

    with open(file=error_report_file, mode=mode) as api_error_report:
        csvwriter: writer = csv.writer(api_error_report)
        if mode == 'w':
            fields_name: list[str] = ["zipcode", "http_status_code", "description", 'api_errors', "retry"]
            csvwriter.writerow(fields_name)
        csvwriter.writerow(data)


def write_staging_report(data: list[dict] | None = None ) -> None:
    """
    This function shall create a staging json file using the data provided.
    It utilized "create_file_name" function to maintain consistency of six hour interval naming.

    :param data:
    :param JSON_FILE_PATH:
    :return:
    """
    staging_file: str = os.path.join(WEATHER_REPORT_STAGING_PATH , create_file_name(weather_reports=True))

    try:
        dir_name = os.path.dirname(staging_file)

        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

        with open(staging_file, 'w') as f:
            json.dump(data, f)
    except OSError as exec:
        print(exec.strerror)
        raise exec