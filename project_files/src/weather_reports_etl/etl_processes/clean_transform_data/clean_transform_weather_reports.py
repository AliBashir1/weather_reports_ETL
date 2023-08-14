from __future__ import annotations

import pandas as pd
from pandas import DataFrame
from typing import List
from src.weather_reports_etl.utilities.log import log

# add lru_cache decorator for faster execution.

@log
def clean_transform_weather_reports(weather_reports: List[dict] | None= None) -> DataFrame:
    """
    A weather reports is a list of dictionary, each dictionary is containing the information of the weather of a zipcode.
    weather reports shall be structured to row and column format with proper name formatting and datatypes.

    :param weather_reports: weather reports from weather api in list of dictionary format.
    :return: returns a structured weather reports in a pandas dataframe.
    """
    drop_columns_list = ['location.name', 'location.region', 'location.country', 'location.lat', 'location.lon',
                         'location.tz_id', 'location.localtime_epoch', 'location.localtime',
                         'current.last_updated_epoch', 'current.last_updated', 'current.temp_c', 'current.is_day',
                         'current.condition.icon', 'current.condition.code', 'current.wind_kph', 'current.pressure_in',
                         'current.precip_in', 'current.feelslike_c', 'current.vis_km', 'current.gust_kph']

    if weather_reports is not None:
        # normalize json format, convert to df and concat it
        weather_df = pd.concat([pd.json_normalize(report) for report in weather_reports])

        # Convert data_type
        weather_df.insert(loc=1, column="local_time", value=pd.to_datetime(weather_df["location.localtime"]))
        weather_df.insert(loc=2, column="last_updated", value=pd.to_datetime(weather_df["current.last_updated"]))

        # adds day name
        weather_df.insert(loc=3, column="day_of_week", value=weather_df["local_time"].dt.day_name())

        # drop columns
        weather_df = weather_df.drop(drop_columns_list, axis=1)

        # clean up column name
        # split converts current.condition.text to ["current", "condition", "text"]
        weather_df.columns = [column[0] if len(column) == 1 else column[1] for column in weather_df.columns.str.split(".")]

        return weather_df
    else:
        raise AttributeError("Weather Report cannot be None")

if __name__ == "__main__":
    import time

    start_time = time.time()
    from project_files.src.weather_reports_etl.connections.api_connection import get_weather_api_session
    from project_files.src.weather_reports_etl.etl_processes.fetch_data.fetch_data_db import create_most_populated_zipcodes_file
    from project_files.src.weather_reports_etl.etl_processes.fetch_data.fetch_data_api import fetch_weather_reports
