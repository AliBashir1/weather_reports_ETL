
import pandas as pd
from connections.mysql_connections import get_mysql_connections
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from pymysql.err import ProgrammingError
import sys


def fetch_most_populated_zipcodes():
    """
    This function runs the sql query to fetch most populated zipcodes group by county for now
    :return: a pandas dataframe.
    """
    most_populated_zipcodes = None

    # a -- is a dataset of zipcodes with no military(Afo, Pfo) and zipcodes with population greater than zero.
    # b -- is a dataset of zipcodes with max population grouped by county and if county is "NA" than city.
    query = """
       SELECT
            c.zipcode
        From (
            SELECT 
                max(a.population) as population,
                a.city_or_county
            FROM (
                SELECT 
                    id,
                    zipcode,
                    population,
                    CASE WHEN county = "NA" THEN city else county end as city_or_county 
                FROM 
                    zipcodes_info
                WHERE zipcode_type != "MILITARY" AND population > 0 ) AS a
                GROUP BY a.city_or_county ) AS b
        INNER JOIN zipcodes_info c
            on b.city_or_county = c.city or b.city_or_county = c.county and b.population = c.population;
    """

    try:
        with get_mysql_connections().connect() as con:
            most_populated_zipcodes = pd.read_sql_query(sql=text(query), con=con, )

    except DBAPIError or ProgrammingError:
        pass
        # todo log it

    if most_populated_zipcodes is not None:
        return most_populated_zipcodes.squeeze()


if __name__ == "__main__":
    import time
    start = time.time()
    a = fetch_most_populated_zipcodes()
    if a is not None:
        a = a["zipcode"].astype(str).str.zfill(5)
    print("execution ended in {}".format(time.time() - start))

