from sqlalchemy.exc import IntegrityError, DBAPIError
from sqlalchemy.engine.base import Engine
from utilities.log import log
from pandas import DataFrame

@log
def load_weather_reports_tbl(weather_reports_df: DataFrame = None, eng: Engine = None ) -> None:
    """Loads weather_reports_df to weather_reports_table"""
    if weather_reports_df is not None:
        try:
            with eng.connect() as con:
                weather_reports_df.to_sql("weather_reports", con=con, if_exists="append", index=False)

        except (Exception, DBAPIError, IntegrityError) as e:
            raise Exception(str(e))
    else:
        raise AttributeError("Attribute reference or assignment failed.")


if __name__ == "__main__":
    import time

    start_time = time.time()
    from connections.api_connection import get_weather_api_session
    from connections.mysql_connections import get_mysql_connections
    from connections.aws_connections import get_s3_client
    from etl_processes.fetch_data.fetch_data_db import fetch_most_populated_zipcodes
    from etl_processes.fetch_data.fetch_data_api import fetch_weather_reports
    from etl_processes.fetch_data.fetch_staging_data_s3 import fetch_daily_reports
    from etl_processes.clean_transform_data.clean_transform_weather_reports import clean_transform_weather_reports

    # zipcodes = fetch_most_populated_zipcodes()
    # session = get_weather_api_session()
    # weather_reports = fetch_weather_reports(zipcodes, session)
    raw_data = fetch_daily_reports(get_s3_client())
    eng = get_mysql_connections()
    for data in raw_data:
        weather_reports_df = clean_transform_weather_reports(data)
        load_weather_reports_tbl(weather_reports_df, eng)



    print("load_weather_report_tbl load time in seconds -- {}".format(time.time() - start_time))

