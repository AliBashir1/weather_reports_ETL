import os
from src.weather_reports_etl.utilities.definitions import ROOT_DIR
import configparser
from configparser import ConfigParser


def get_config_parser() -> ConfigParser:
    """Reads config_file.ini file, initiates ConfigParses and returns it"""
    # get an absolute path
    config_file= os.path.join(ROOT_DIR, "config", "conf.ini")
    # config_file = os.path.abspath(config_file_path)
    config = configparser.ConfigParser()
    config.read(config_file)

    return config


if __name__ == "__main__":
    a = get_config_parser()
    weather_db_conn_str = a.get("WEATHER_DB", "WEATHER_DB_CONN")
    print(weather_db_conn_str)
