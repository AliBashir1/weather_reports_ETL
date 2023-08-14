import os
from botocore.exceptions import ClientError, HTTPClientError

from src.weather_reports_etl.connections.aws_connections import get_s3_client
from src.weather_reports_etl.utilities.files import WEATHER_REPORT_STAGING_PATH, create_file_name


def load_to_aws_s3_bucket() -> None:
    """Loading staging data to aws s3 bucket."""
    bucket_name = "ab-weather-reports"
    key = create_file_name()
    with open(
            os.path.join(WEATHER_REPORT_STAGING_PATH, create_file_name()),
            'rb') as staging_data:
        try:
            s3_client = get_s3_client()

            s3_client.put_object(Bucket=bucket_name, Key=key, Body=staging_data, ACL='private')
        except ClientError as e:
            # todo log it
            print(e.response["Error"])
            raise e
        except HTTPClientError as e:
            # todo log it
            raise e


# # extra
# def validate_bucket(s3_client: BaseClient, bucket_name: str) -> bool:
#     """Validation aws bucket name and if it is available or not."""
#     # check bucket naming convention
#     bucket_name_pattern = "(?!(^((2(5[0-5]|[0-4][0-9])|[01]?[0-9]{1,2})\.){3}(2(5[0-5]|[0-4][0-9])|[01]?[0-9]{1,2})" \
#                           "$|^xn--|.+-s3alias$))^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]"
#     try:
#         if re.fullmatch(bucket_name_pattern, bucket_name):
#             resp = s3_client.head_bucket(Bucket=bucket_name)
#
#             # Check HTTPStatus code
#             status_code = resp["ResponseMetadata"]["HTTPStatusCode"]
#             return True if status_code == 200 else False
#     except ClientError as e:
#         # todo log it
#         print(e)
#
#
# def create_bucket(bucket_name: str) -> bool:
#     """Creating Bucket"""
#
#     s3_client = get_s3_client()
#     # check if bucket_name is correct and doesn't exists
#     try:
#         if validate_bucket(s3_client, bucket_name):
#             return False
#         else:
#             resp = s3_client.create_bucket(Bucket=bucket_name, ACL="private")
#             return True if resp["ResponseMetadata"]["HTTPStatusCode"] == 200 else False
#     except ClientError as e:
#         print(e)


if __name__ == "__main__":
   load_to_aws_s3_bucket()

