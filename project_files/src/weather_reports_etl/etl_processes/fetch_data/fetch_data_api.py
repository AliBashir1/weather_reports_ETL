from __future__ import annotations
import os
import pandas as pd
from pandas import Series
from requests import Session
from requests.exceptions import HTTPError, RetryError

from src.weather_reports_etl.utilities.files import (ZIPCODES_FILEPATH,
                                                        create_file_name,
                                                        ERROR_REPORT_PATH
                                                         )
from src.weather_reports_etl.utilities.log import log
from src.weather_reports_etl.utilities.files import write_staging_report
from src.weather_reports_etl.connections.api_connection import get_weather_api_session
from src.weather_reports_etl.utilities.files import write_error_report

def get_invalid_zipcodes_count()-> int:
    """
    Check if error exists for current six hour interval, reads the file and get the count of invalid zipcodes and returns it
    :return: a count of invalid zipcodes
    """
    invalid_zipcode_error: list[int] =[1006]
    invalid_zipcodes_count: int = 0
    error_report_file: str = os.path.join(ERROR_REPORT_PATH, create_file_name(weather_reports=False))
    if os.path.exists(error_report_file):
        error_df = pd.read_csv(error_report_file)
        error_mask = error_df["api_errors"].isin(invalid_zipcode_error)
        invalid_zipcodes_count = error_df[error_mask].shape[0]
    return invalid_zipcodes_count


@log
def fetch_weather_reports() -> dict:
    """
    This is a main function for hourly_weather_reports an airflow job. It uses zipcodes.csv data and make an HTTP request for each
    zipcode. Add these json responses to weather_reports list.weather_reports list gets passed to "write_staging_file"
    that will create a staging file for current six hour interval.It will return a dictionary with possible invalid
    zipcodes or any error via api, number of total zipcodes from source,successful api request count and over all
    job status.

    Information about exceptions:
    The weather api returns an error code 1006 that means the provided zipcode is invalid, and it is only the only
    information required for analysis beside that rest of the HTTPError are lethal, and will raise an exception.

    Retry Exceptions are the exception that may be caused by session timeout or server timeout etc. Api session shall retry
    to connect again after three tries, it shall fail and raise the exception.

    RETURNING DATA INFORMATION
    The returning data is used in airflow job for updating  "hourly_job_info" and send status report via email.
    look at "hourly_job_info" for further detail.

    :return: a dictionary type with information of the process it ran.
    """

    URL: str = "https://weatherapi-com.p.rapidapi.com/current.json"
    weather_reports: list[dict] = []
    successful_zipcodes_count: int = 0
    session: Session = get_weather_api_session()
    zipcodes_series: Series  = pd.read_csv(ZIPCODES_FILEPATH,
                           header=None,
                           skiprows=1).squeeze()
    total_zipcodes: int  = zipcodes_series.shape[0]

    if zipcodes_series is not None:
        # Api takes 5 digits, some zipcodes are less than 5, add leading 0 to it
        zipcodes_series = zipcodes_series.astype(str).str.zfill(5)
        for _, zipcode in zipcodes_series.items():
            try:
                response = session.get(URL, params={'q': zipcode})
                # raise for any bad http response
                response.raise_for_status()

                # Create Data
                data = response.json()
                data["zipcode"] = zipcode
                weather_reports.append(data)

                successful_zipcodes_count += 1
            # If there is an error, log it.
            except HTTPError as exec:
                write_error_report(zipcode=zipcode,
                                   http_status_code=exec.response.status_code,
                                   description=exec.strerror,
                                   api_errors=1006, retry='No'
                                   )
                # 1006 represent invalid zipcode, which can be handled later.
                if (exec.response.status_code == 400
                        and exec.response.json()["error"]["code"] == 1006):
                    continue
                else:
                    raise exec

            except RetryError as exec:
                write_error_report(zipcode=zipcode,
                                   http_status_code=None,
                                   description=exec.strerror,
                                   api_errors=None,
                                   retry='Yes' )
                continue

    else:
        raise FileNotFoundError("{} is not found at {}"
                            .format(os.path.basename(ZIPCODES_FILEPATH),
                                    ZIPCODES_FILEPATH))

    # Checks if weather_reports are not None
    if weather_reports:
        write_staging_report(weather_reports)
        invalid_zipcodes_count = get_invalid_zipcodes_count()

        return { "invalid_zipcodes_count": invalid_zipcodes_count,
                 "successful_zipcodes_count":  successful_zipcodes_count,
                 "total_zipcodes": total_zipcodes,
                 "job_status": 'Partially Successful' if invalid_zipcodes_count > 0 else 'Successful'

                }
    else:
        raise AttributeError("weather_reports reference or assignment failed.")



if __name__ == "__main__":
    import time
    from src.weather_reports_etl.connections.api_connection import get_weather_api_session

    start_time = time.time()
    a = fetch_weather_reports()
    print("fetch_weather_reports time -- {}".format(time.time() - start_time))
