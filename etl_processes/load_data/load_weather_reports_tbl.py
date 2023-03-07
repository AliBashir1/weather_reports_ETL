from connections.mysql_connections import get_mysql_connections
from etl_processes.clean_transform_data.clean_transform_weather_reports import clean_transform_weather_reports
from sqlalchemy.exc import IntegrityError, DBAPIError
import time
from utilities.log import log


def load_weather_reports_tbl() -> None:
    try:
        with get_mysql_connections().connect() as con:
            weather_reports_df = clean_transform_weather_reports()
            weather_reports_df.to_sql("weather_reports", con=con, if_exists="fail", index=False)

    except (Exception, DBAPIError, IntegrityError) as e:
        raise Exception("Exception")


if __name__ == "__main__":
    start_time = time.time()
    a = load_weather_reports_tbl()

    print("load_weather_report_tbl load time in seconds -- {}".format(time.time() - start_time))