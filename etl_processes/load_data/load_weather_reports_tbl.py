from connections.mysql_connections import get_mysql_connections
from etl_processes.clean_transform_data.clean_transform_weather_reports import clean_transform_weather_reports
from sqlalchemy.exc import IntegrityError
import logging
import time
import sys


def load_weather_reports_tbl():
    try:
        with get_mysql_connections().connect() as con:
            weather_reports_df = clean_transform_weather_reports()
            weather_reports_df.to_sql("weather_reports", con=con, if_exists="fail", index=False)
    except IntegrityError:
        logging.error(sys.exc_info())


if __name__ == "__main__":
    # testing log feature
    logging.basicConfig(format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s",
                        level=logging.DEBUG)
    logging.info("The process of loading weather reports is starting")
    start_time = time.time()
    load_weather_reports_tbl()
    logging.info("Current weather reports have been added to weather_reports_tbl")
    print("load_weather_report_tbl load time in seconds -- {}".format(time.time() - start_time))