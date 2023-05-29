import datetime as dt
import pytz
import re
from botocore.exceptions import ClientError
from botocore.client import BaseClient
import json


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


def load_weather_staging_data_s3(s3_client: BaseClient, staging_data: list) -> bool:
    """Loading staging data to aws s3 bucket."""
    bucket_name = "staging-data-ab"
    data = json.dumps(staging_data)
    now = dt.datetime.now(pytz.timezone("America/New_York"))

    # weather-reports/2023-05-24/03-PM

    key = f"weather-reports/{now.strftime('%Y-%m-%d')}/{now.strftime('%I-%p')}"

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
    from connections.aws_connections import get_s3_client
    from connections.api_connection import get_weather_api_session
    from connections.mysql_connections import get_mysql_connections

    # data fetchers and loaders
    from etl_processes.fetch_data.fetch_data_db import fetch_most_populated_zipcodes
    from etl_processes.fetch_data.fetch_data_api import fetch_weather_reports

    s3_client = get_s3_client()
    api_session = get_weather_api_session()
    sql_engine = get_mysql_connections()

    # load to staging
    zipcodes = fetch_most_populated_zipcodes()
    weather_reports = fetch_weather_reports(zipcodes, api_session)
    load_weather_staging_data_s3(s3_client, weather_reports)
