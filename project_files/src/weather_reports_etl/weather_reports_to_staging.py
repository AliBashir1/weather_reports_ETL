from project_files.src.weather_reports_etl.etl_processes.fetch_data.fetch_data_db import fetch_most_populated_zipcodes
from project_files.src.weather_reports_etl.etl_processes.fetch_data.fetch_data_api import fetch_weather_reports
from project_files.src.weather_reports_etl.etl_processes import load_weather_staging_data_s3

from sqlalchemy.exc import DBAPIError
from pymysql.err import ProgrammingError
from requests.exceptions import HTTPError
from botocore.exceptions import ClientError
from pandas import Series


def load_weather_reports_to_staging():
    try:
        zipcodes: Series = fetch_most_populated_zipcodes()
        if ~zipcodes.empty:
            weather_reports = fetch_weather_reports(zipcodes=zipcodes)
            if weather_reports:
                load_weather_staging_data_s3(staging_data=weather_reports)

    except DBAPIError or ProgrammingError as dbe:
        print("Error in fetch_most_populated_zipcodes: {}".format(dbe))
    except HTTPError as he:
        print("Error in fetch_weather_reports: {}".format(he))
    except ClientError as ce:
        print("Error in load_weater_staging_data_se: {}".format(ce))


if __name__ == "__main__":
    load_weather_reports_to_staging()
