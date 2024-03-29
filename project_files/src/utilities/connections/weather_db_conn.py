from functools import lru_cache
from configparser import ConfigParser

from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine

from src.utilities.encryptions import __decrypt
from src.utilities.config_parser import get_config_parser



@lru_cache
def get_weather_db_conn() -> Engine:
    """
    Initiate SQL Alchemy Engine with configuration and returns it
    :return: An SQL Alchemy Engine.
    """
    config: ConfigParser = get_config_parser()

    # Encrypted connection string
    con_str: str = config.get("WEATHER_DB", "WEATHER_DB_CONN")

    # decrypt connection string using key
    d_con_str: str = __decrypt(con_str)
    # Initiate SQL Alchemy Engine
    engine = create_engine(d_con_str)
    return engine


if __name__ == "__main__":
    a = get_weather_db_conn()
    with a.connect() as con:
        a = con.execute(text("Select * from zipcodes_tbl;"))
        print(a.fetchall())
