from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from src.weather_reports_etl.utilities.encryptions import __decrypt
from src.weather_reports_etl.utilities.config_parser import get_config_parser
from functools import lru_cache
from src.weather_reports_etl.utilities.log import log


# add lru_cache to reduce function execution time
@lru_cache
@log
def get_weather_db_conn() -> Engine:
    """Initiate SQL Alchemy Engine with configuration and returns it"""
    config = get_config_parser()

    # Encrypted connection string
    con_str = config.get("WEATHER_DB", "WEATHER_DB_CONN")

    # decrypt connection string using key
    d_con_str = __decrypt(con_str)
    # Initiate SQL Alchemy Engine
    engine = create_engine(d_con_str)
    return engine


if __name__ == "__main__":
    a = get_weather_db_conn()
    with a.connect() as con:
        a = con.execute(text("Select * from zipcodes_tbl;"))
        print(a.fetchall())
