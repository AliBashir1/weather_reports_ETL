import pandas as pd
from src.weather_reports_etl.connections.weather_db_conn import get_weather_db_conn
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from pymysql.err import ProgrammingError
from src.weather_reports_etl.utilities.log import log
from pandas import Series


@log
def fetch_most_populated_zipcodes() -> bool:
    """Query zipcodes_info to fetch most populated zipcodes in a county, converts to Series and returns it"""
    query = """
            WITH partitioned_data AS (
            -- Rank zipcode within state and county by population
                SELECT 
                    zipcode, 
                    ROW_NUMBER() OVER( PARTITION BY state, county  
                                        ORDER BY population DESC ) AS population_rank_by_state_county 
                FROM zipcodes_tbl   
                    WHERE population > 0 
                    )
   
            SELECT zipcode 
            FROM partitioned_data 
                WHERE population_rank_by_state_county=1 ;
    """
    try:
        with get_weather_db_conn().connect() as con:
            most_populated_zipcodes = pd.read_sql_query(sql=text(query), con=con)



    except DBAPIError or ProgrammingError as e:
        raise e

    if most_populated_zipcodes is not None:
        most_populated_zipcodes.to_csv("airflow-worker:/project_files/src/weather_reports_etl/data/zipcodes.csv", index=False)
        return True


if __name__ == "__main__":
    import time

    start = time.time()
    fetch_most_populated_zipcodes()
    # print(a["zipcode"])
    # if a is not None:
    #     a = a["zipcode"].astype(str).str.zfill(5)
    print("execution ended in {}".format(time.time() - start))
