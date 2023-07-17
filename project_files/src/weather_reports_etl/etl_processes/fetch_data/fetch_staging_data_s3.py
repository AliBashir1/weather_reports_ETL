import datetime
import json
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from datetime import timedelta
from src.weather_reports_etl.utilities.log import log
# there are going to be 4 reports on daily basis
# 12 am,  6 am, 12 pm, 6pm
# need to fetch all 4 objects from s3 bucket
# pass it to clean_transform_data

@log
def fetch_daily_staging_data(s3_client: BaseClient) -> list:
    """Fetch staging reports from aws s3 staging buckets and load into temporary folder"""

    bucket_name = "staging-data-ab"
    # previous day's report
    keys = [f"weather-reports/{datetime.date.today() - timedelta(days=1)}/12-AM",
            f"weather-reports/{datetime.date.today() - timedelta(days=1)}/06-AM",
            f"weather-reports/{datetime.date.today() - timedelta(days=1)}/12-PM",
            f"weather-reports/{datetime.date.today() - timedelta(days=1)}/06-PM"]

    staging_data = []
    try:
        for key in keys:
            print(f"fetching data of{key}")

            # fetch binary data
            binary_data = s3_client.get_object(Bucket=bucket_name, Key=key)['Body'].read()

            # convert to json
            data = json.loads(binary_data)

            # add into staging data. data is nested list of dictionary.
            staging_data.append(data)
        return staging_data
    except ClientError as e:
        print(e)


if __name__ == '__main__':

    from project_files.src.weather_reports_etl.connections.aws_connections import  get_s3_client
    from project_files.src.weather_reports_etl.etl_processes.clean_transform_data.clean_transform_weather_reports import clean_transform_weather_reports

    s3_client = get_s3_client()
    data = fetch_daily_staging_data(s3_client)
    # cleaned_data = clean_transform_weather_reports(data)
    for d in data:
        cleaned_data = clean_transform_weather_reports(d)
        print(len(cleaned_data))

