from __future__ import annotations
import os
from src.weather_reports_etl.utilities.files import ROOT_DIR
import configparser
from configparser import ConfigParser


def get_config_parser() -> ConfigParser:
    """
    This function reads a conf.ini file, create config parses and returns it.
    :return: a config parser
    """
    # get an absolute path
    config_file: str = os.path.join(ROOT_DIR, "config", "conf.ini")
    config: ConfigParser | None = configparser.ConfigParser()
    config.read(config_file)

    if config is not None:
        return config
    else:
        raise ValueError("Config is none type.")


if __name__ == "__main__":
    a = get_config_parser()
    weather_db_conn_str = a.get("WEATHER_DB", "WEATHER_DB_CONN")
    print(weather_db_conn_str)
