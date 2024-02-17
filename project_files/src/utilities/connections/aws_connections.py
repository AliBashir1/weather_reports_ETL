import boto3
from botocore.client import Config, BaseClient
from botocore.exceptions import ClientError

from src.utilities.config_parser import get_config_parser
from src.utilities.encryptions import __decrypt


def get_s3_client() -> BaseClient:
    """
    Initiate a s3 client using  credentialn from conf.ini file and return it
    :return: a s3 connection client.
    """
    # get aws access id and key
    config_parser = get_config_parser()
    access_id = __decrypt(config_parser.get("AWS", "aws_access_key_id"))
    access_key = __decrypt(config_parser.get("AWS", "aws_secret_access_key"))

    config = Config(
        connect_timeout=120,
        read_timeout=120,
        region_name='us-east-1',
        retries={"max_attempts": 3}
    )

    # configure s3 client
    try:
        s3_client = boto3.client("s3",
                                 config=config,
                                 aws_access_key_id=access_id,
                                 aws_secret_access_key=access_key
                                 )

        if s3_client is not None:
            return s3_client
        else:
            raise ClientError
    except ClientError as e:
        print(e)


if __name__ == "__main__":
    s3_client = get_s3_client()
    s3_client.create_bucket(Bucket="what-is-is-this")
