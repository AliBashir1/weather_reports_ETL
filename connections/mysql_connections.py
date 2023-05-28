from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from utilities.encryptions import __decrypt
from utilities.config_parser import get_config_parser

# add lru_cache to reduce function execution time
def get_mysql_connections() -> Engine:
    """Initiate SQL Alchemy Engine with configuration and returns it"""
    config = get_config_parser()

    # Encrypted connection string and key
    con_str = config.get("MYSQLDB", "MYSQL_AD_CON")
    key = config.get("KEY", "KEY")

    # decrypt connection string using key
    d_con_str = __decrypt(key, con_str)

    # Initiate SQL Alchemy Engine.
    engine = create_engine(d_con_str)
    return engine


if __name__ == "__main__":
    a = get_mysql_connections()
    with a.connect() as con:
        a = con.execute(text("Select * from zipcodes_info;"))
        print(a.fetchall())
