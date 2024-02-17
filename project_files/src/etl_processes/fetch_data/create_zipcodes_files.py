import pandas as pd
from pymysql.err import ProgrammingError
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from src.utilities.connections.weather_db_conn import get_weather_db_conn
from src.utilities.files import ZIPCODES_FILEPATH


def create_most_populated_zipcodes_file() -> bool:
    """
    Function shall create a csv file containing the most populated zipcodes grouped by state and county.
    The inner query shall rank in ascending order from most populated to the least populated by county in state.
    The outer query shall pick the top on each grouped, then itll create a csv file using pandas and returns the
    conformation.
    :return: a boolean flag indicating that the file has been created or not.
    """
    query = """
            WITH partitioned_data AS (
            -- Rank zipcode within state and county by population
                SELECT 
                    zipcode, 
                    ROW_NUMBER() OVER( PARTITION BY state, county  
                                        ORDER BY population DESC ) AS population_rank_by_state_county 
                FROM zipcodes_tbl   
                    WHERE population > 0 and zipcode_type = 'STANDARD' 
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
        most_populated_zipcodes.to_csv(ZIPCODES_FILEPATH, index=False)
        return True


if __name__ == "__main__":
    import time

    start = time.time()
    create_most_populated_zipcodes_file()

    print("execution ended in {}".format(time.time() - start))
