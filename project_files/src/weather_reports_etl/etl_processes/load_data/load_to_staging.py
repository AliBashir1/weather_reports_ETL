import datetime as dt
import pytz
import re
from botocore.exceptions import ClientError
from botocore.client import BaseClient
import json
from src.weather_reports_etl.connections.aws_connections import get_s3_client


def validate_bucket(s3_client: BaseClient, bucket_name: str) -> bool:
    """Validation aws bucket name and if it is available or not."""
    # check bucket naming convention
    bucket_name_pattern = "(?!(^((2(5[0-5]|[0-4][0-9])|[01]?[0-9]{1,2})\.){3}(2(5[0-5]|[0-4][0-9])|[01]?[0-9]{1,2})" \
                          "$|^xn--|.+-s3alias$))^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]"
    try:
        if re.fullmatch(bucket_name_pattern, bucket_name):
            resp = s3_client.head_bucket(Bucket=bucket_name)

            # Check HTTPStatus code
            status_code = resp["ResponseMetadata"]["HTTPStatusCode"]
            return True if status_code == 200 else False
    except ClientError as e:
        print(e)


def __create_s3_key():
    now = dt.datetime.now(pytz.timezone("America/New_York"))
    hour = now.time().hour
    key_hour = None
    if 0 <= hour < 6:
        key_hour = "12-AM"
    if 6 <= hour < 12:
        key_hour = "06-AM"
    elif 12 <= hour < 18:
        key_hour = '12-PM'
    elif 18 <= hour <= 24:
        key_hour = "06-PM"
    if key_hour is not None:

        # weather-reports/2023-05-24/03-PM
        key = f"weather-reports/{now.strftime('%Y-%m-%d')}/{key_hour}"
        return key


def load_weather_staging_data_s3(*, staging_data: list) -> bool:
    """Loading staging data to aws s3 bucket."""
    bucket_name = "weather-reports-staging"
    data = json.dumps(staging_data)
    s3_client = get_s3_client()
    key = __create_s3_key()
    try:
        resp = s3_client.put_object(Bucket=bucket_name, Key=key, Body=data, ACL='private')
        return True if resp["ResponseMetadata"]["HTTPStatusCode"] == 200 else False
    except ClientError as e:
        print(e)


def create_bucket(bucket_name: str) -> bool:
    """Creating Bucket"""

    s3_client = get_s3_client()
    # check if bucket_name is correct and doesn't exists
    try:
        if validate_bucket(s3_client, bucket_name):
            return False
        else:
            resp = s3_client.create_bucket(Bucket=bucket_name, ACL="private")
            return True if resp["ResponseMetadata"]["HTTPStatusCode"] == 200 else False
    except ClientError as e:
        print(e)


if __name__ == "__main__":
    # connections
    __create_s3_key()
    # from weather_reports_project.connections.aws_connections import get_s3_client
    # from weather_reports_project.connections.api_connection import get_weather_api_session
    # from weather_reports_project.connections.mysql_connections import get_mysql_connections
    #
    # # data fetchers and loaders
    # from weather_reports_project.etl_processes.fetch_data.fetch_data_db import fetch_most_populated_zipcodes
    # from weather_reports_project.etl_processes.fetch_data.fetch_data_api import fetch_weather_reports
    #
    # s3_client = get_s3_client()
    # api_session = get_weather_api_session()
    # sql_engine = get_mysql_connections()
    #
    # # load to staging
    # zipcodes = fetch_most_populated_zipcodes()
    # weather_reports = fetch_weather_reports(zipcodes, api_session)
    # load_weather_staging_data_s3(s3_client, weather_reports)
