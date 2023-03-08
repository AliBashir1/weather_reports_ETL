import pandas as pd
from pandas import DataFrame
import time
from utilities.definitions import ROOT_DIR
import os
from sqlalchemy.exc import DBAPIError, SQLAlchemyError, IntegrityError
from sqlalchemy.engine.base import Engine

from utilities.log import log

"""
Following methods are going to be run only one time to ensure that data is cleaned and loaded into database.
"""


@log
def _clean_zipcodes_info() -> DataFrame:
    """Creates dataframe from zipcode.csv file, format and return a dataframe."""
    # build a path to the file.
    filename = "zip_code_database.csv"
    zipcode_file_path = os.path.join(ROOT_DIR, "in", filename)

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


@log
def load_zipcodes_info_tbl(zipcode_df: DataFrame, con: Engine = None) -> None:
    """Loads zipcode_info tables"""
    try:
        with con.connect() as con:
            zipcode_df.to_sql(name="zipcodes_info", if_exists="append", con=con, index=False)

            # if does_tbl_exist(table_name="zipcodes_info"):
            #     q = "ALTER TABLE zipcodes_info ADD COLUMN `id` INT PRIMARY KEY AUTO_INCREMENT FIRST;"
            #     con.execute(q)
    except (ValueError, DBAPIError, SQLAlchemyError, IntegrityError) as e:
        raise e


@log
def does_tbl_exist(table_name: str, schema_name: str = "zipcodes_db", con: Engine = None) -> bool:
    """Checks if the table exists."""
    query = """
    SELECT count(*)
    FROM information_schema.TABLES
        WHERE (TABLE_SCHEMA = "{}") 
            AND (TABLE_NAME = "{}")
    """.format(schema_name, table_name)

    with con.connect() as con:
        results = con.execute(query).first()[0]
        if results == 1:
            return True

    return False


@log
def does_db_exist(con: Engine = None) -> bool:
    """Checks if the database exists"""
    query = """
        SHOW databases LIKE "zipcodes_db"
    """
    with con.connect() as con:
        results = con.execute(query).first()[0]
        if results == "zipcodes_db":
            return True

    return False


if __name__ == "__main__":
    from connections.mysql_connections import get_mysql_connections

    start_time = time.time()
    zipcode_df = _clean_zipcodes_info()
    # a = zipcode_df.iloc[307:310]
    # print(a)
    con = get_mysql_connections()
    load_zipcodes_info_tbl(zipcode_df, con)

    print("%s seconds ---" % (time.time() - start_time))
