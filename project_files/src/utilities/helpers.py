from __future__ import annotations
import datetime as dt
import pytz
from airflow.utils.context import Context
from dateutil import parser
import math

def format_date_filter(utc_date: dt.datetime | str = None) -> str | None:
    if isinstance(utc_date, str):
        return parser.parse(utc_date).astimezone(pytz.timezone("America/New_York")) \
            .strftime("%Y-%m-%d %I:%M:%S %p ")
    elif isinstance(utc_date, dt.datetime):
        return utc_date.astimezone(pytz.timezone("America/New_York")) \
            .strftime("%Y-%m-%d %I:%M:%S %p ")
    else:
        return None

# only deals with start_end and end_time from dag_run table
def total_execution_time(start_time: str = None, end_time: str = None) -> str:
    if isinstance(start_time, str):
       start_time= parser.parse(start_time)
    if isinstance(end_time, str):
        end_time = parser.parse(end_time)

    delta = end_time - start_time
    hours = math.floor(delta.total_seconds() / 3600)
    minutes = math.floor(delta.total_seconds() / 60 - hours * 60)
    seconds = round(delta.total_seconds() - (minutes * 60) - (hours * 3600), 2)
    if hours <= 0:
        if minutes <= 0:
            if seconds > 0:
                return "{} seconds".format(seconds)
        else:
            return "{} minutes and {} seconds".format(minutes, seconds)
    else:
        return "{} hours, {} minutes and {} seconds".format(hours, minutes, seconds)

def print_today_date():
    today = dt.datetime.today()
    return dt.datetime.strftime(today, "%b %d, %Y")


def push_return_value(context: Context) -> None:
    """ This function shall push the data into xcom variables coming from python callable's return value.
    The "return value" is a dictionary holding information process in the tasks.
     for example "fetch_weather_reports_api" returns { "request_count": request_count} """
    task_id = context["ti"].task_id
    result: dict | None  = context["ti"].xcom_pull(task_ids=task_id)

    if result is not None:
        for key, value in result.items():
            context["ti"].xcom_push(key=key, value=value)
    else:
        raise ValueError("return value cannot be None.")