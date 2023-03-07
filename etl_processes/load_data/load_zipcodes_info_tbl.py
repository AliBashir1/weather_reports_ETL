import datetime
import pandas as pd
from pandas import DataFrame
import time
from connections.mysql_connections import get_mysql_connections
from config.definitions import ROOT_DIR
import os
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
import logging

from utilities.log import log

"""
Following methods are going to be run only one time to ensure that data is cleaned and loaded into database.
"""

# LOG_FILE = "/Users/alibashir/Desktop/workspace/ETL/weather_data/log/log_{}.log".format(datetime.datetime.now())


def _clean_zipcodes_info() -> DataFrame:
    """
    This function loads the data from zip_code_database.csv file to pandas df, renames and cleans the columns name
    and returns it.
    :return: a pandas dataframe type.
    """
    # build a path to the file.
    filename = "zip_code_database.csv"
    zipcode_file_path = os.path.join(ROOT_DIR, "in", filename)

    # logging.debug("{} is being prepared to load into zipcodes_db.zipcodes_info".format(zipcode_file_path))

    zipcode_df = pd.read_csv(zipcode_file_path,
                             usecols=["zip",
                                      "type",
                                      "primary_city",
                                      "county",
                                      "state",
                                      "country",
                                      "irs_estimated_population",
                                      "area_codes", "timezone"]
                             )
    # assign new column name
    new_names = {"zip": "zipcode",
                 "type": "zipcode_type",
                 "primary_city": "city",
                 "irs_estimated_population": "population"
                 }
    zipcode_df = zipcode_df.rename(mapper=new_names, axis=1)

    # clean up some values
    zipcode_df["county"] = (zipcode_df["county"]
                            .str.replace("County", "")
                            .str.replace("Municipio", "").fillna("NA")
                            )
    return zipcode_df


def load_zipcodes_info_tbl(zipcode_df: DataFrame) -> None:
    """
    This function loads data into mysql table zipcodes_info
    :return: None
    """
    try:
        with get_mysql_connections().connect() as con:
            rows_affected = zipcode_df.to_sql(name="zipcodes_info", if_exists="fail", con=con, index=False)
    except (ValueError,DBAPIError, SQLAlchemyError) as e:
        raise e


def does_tbl_exist(table_name: str, schema_name: str = "zipcodes_db") -> bool:
    query = """
    SELECT count(*)
    FROM information_schema.TABLES
        WHERE (TABLE_SCHEMA = "{}") 
            AND (TABLE_NAME = "{}")
    """.format(schema_name, table_name)

    with get_mysql_connections().connect() as con:
        results = con.execute(query).first()[0]
        if results == 1:
            return True

    return False


def does_db_exist() -> bool:
    query = """
        SHOW databases LIKE "zipcodes_db"
    """
    with get_mysql_connections().connect() as con:
        results = con.execute(query).first()[0]
        if results == "zipcodes_db":
            return True

    return False


if __name__ == "__main__":
    start_time = time.time()
    zipcode_df = _clean_zipcodes_info()
    # a = zipcode_df.iloc[307:310]
    # print(a)
    load_zipcodes_info_tbl(zipcode_df)

    print("%s seconds ---" % (time.time() - start_time))
