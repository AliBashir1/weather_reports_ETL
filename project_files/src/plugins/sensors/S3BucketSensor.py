import re
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# https://medium.com/@soniagoyal/how-to-create-a-custom-airflow-sensor-bc5293bd96f3
class S3BucketSensor(BaseSensorOperator):
    def __init__(self, bucket_name, **kwargs):
        if self.validate_bucket_name(bucket_name):
            self.bucket_name = bucket_name
        else:
            raise ValueError("{} name is invalid. Check aws s3 bucket naming convention.".format(bucket_name))
        self.aws_s3_hook = None
        super().__init__(**kwargs)

    @staticmethod
    def validate_bucket_name(bucket_name) -> bool:
        # check bucket naming convention
        bucket_name_pattern = "(?!(^((2(5[0-5]|[0-4][0-9])|[01]?[0-9]{1,2})\.){3}(2(5[0-5]|[0-4][0-9])|[01]?[0-9]{1,2})" \
                              "$|^xn--|.+-s3alias$))^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]"
        if re.fullmatch(bucket_name_pattern, bucket_name):
            return True
        else:
            return False

    def poke(self, context: Context) -> bool:
        self.aws_s3_hook = S3Hook(aws_conn_id="weather_aws_conn")
        return True if self.aws_s3_hook.check_for_bucket(self.bucket_name) else False


