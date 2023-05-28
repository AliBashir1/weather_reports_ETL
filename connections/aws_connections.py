import boto3
from botocore.exceptions import ClientError
from botocore.client import Config, BaseClient
from utilities.config_parser import get_config_parser
from utilities.encryptions import __decrypt
from utilities.log import log


@log
def get_s3_client() -> BaseClient:
    # get aws access id and key
    config_parser = get_config_parser()
    access_id = config_parser.get("AWS", "aws_access_key_id")
    access_key = config_parser.get("AWS", "aws_secret_access_key")

    config = Config(
        connect_timeout=120,
        read_timeout=120,
        region_name='us-east-1',
        retries={"max_attempts": 3}
    )

    # configure s3 client
    try:
        s3_client = boto3.client("s3",
                                 aws_access_key_id=__decrypt(access_id),
                                 aws_secret_access_key=__decrypt(access_key),
                                 config=config
                                 )

        return s3_client
    except ClientError as e:
        print(e)


if __name__ == "__main__":
    s3_client = get_s3_client()
    s3_client.create_bucket(Bucket="what_is_this")
