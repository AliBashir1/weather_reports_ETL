import datetime
import pandas as pd
from pandas import DataFrame
from typing import List
from utilities.log import log


@log
def clean_transform_weather_reports(weather_reports: List[dict] = None) -> DataFrame:
    """Transforms Weather_reports to Pandas Dataframe, adds fields like day_of_week,local_time,etc """
    day_name = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday"
    }
    weather_list: list = []
    zipcode_list: list = []
    local_time_list: list = []
    condition_list: list = []
    day_of_week_list: list = []

    if weather_reports is not None:
        # Current is json for weather details, dataframe needs it keys as column values to store data.
        column_names = [key for key in weather_reports[0]["current"].keys()]

        # loop through weather_reports, create a multi dimensional list weather_df
        # add corresponding zipcode to zipcode list, which will be added as column in weather_df
        for data in weather_reports:
            weather_list.append([value for value in data["current"].values()])
            zipcode_list.append(data["zipcode"])
            local_time_list.append(data["location"]["localtime"])
            condition_list.append(data["current"]["condition"]["text"])
            day = datetime.datetime.fromtimestamp(data["location"]["localtime_epoch"]).weekday()
            day_of_week_list.append(day_name[day])

        # Following columns are required in weather_reports_data set.
        column_list = ["last_updated",
                       "temp_f",
                       "condition",
                       "wind_mph",
                       "wind_degree",
                       "wind_dir",
                       "pressure_mb",
                       "precip_mm",
                       "humidity",
                       "cloud",
                       "feelslike_f",
                       "vis_miles",
                       "uv",
                       "gust_mph"
                       ]
        
        # Create dataframe from multi dimension list, and fetch only columns needed.
        weather_df = pd.DataFrame(data=weather_list, columns=column_names)[column_list]

        # Add zipcode and localtime
        weather_df.insert(loc=0, column="zipcode", value=zipcode_list)
        weather_df.insert(loc=1, column="local_time", value=local_time_list)
        weather_df.insert(loc=3, column="day_of_week", value=day_of_week_list)

        # change columns to appropriate dtypes and changed the name
        weather_df["last_updated"] = pd.to_datetime(weather_df["last_updated"])
        weather_df["local_time"] = pd.to_datetime(weather_df["local_time"])
        weather_df["condition"] = condition_list
        return weather_df
    else:
        raise AttributeError("Attribute reference or assignment failed.")


if __name__ == "__main__":
    import time
    start_time = time.time()
    from connections.api_connection import get_weather_api_session
    from etl_processes.fetch_data.fetch_data_db import fetch_most_populated_zipcodes
    from etl_processes.fetch_data.fetch_data_api import fetch_weather_reports

    zipcodes = fetch_most_populated_zipcodes()
    session = get_weather_api_session()
    weather_reports = fetch_weather_reports(zipcodes, session)
    weather_reports_df = clean_transform_weather_reports(weather_reports)
    print(time.time() -start_time)
    print(weather_reports_df)

