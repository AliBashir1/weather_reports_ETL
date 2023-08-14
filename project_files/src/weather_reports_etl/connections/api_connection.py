from __future__ import annotations
import requests
from requests.sessions import Session
from requests.adapters import HTTPAdapter, Retry
from configparser import ConfigParser

from src.weather_reports_etl.utilities.config_parser import get_config_parser
from src.weather_reports_etl.utilities.encryptions import __decrypt


def get_weather_api_credential() -> dict:
    """
    A dictionary of weather api credential shall be return by using credential from config.ini.
    :return: a dictionary of api credential
    """
    config: ConfigParser | None = get_config_parser()
    # get api host and encrypted key
    host: str | None = config.get("API", "x-rapidapi-host")
    api_key: str | None = __decrypt(config.get("API", "x-rapidapi-key"))

    if host is not None and api_key is not None:
        return {
            "x-rapidapi-host": host,
            "x-rapidapi-key": api_key

        }
    else:
        raise ValueError("None type in api_key and host.")

def get_weather_api_session() -> Session:
    """
    A weather api session is created using api credential from "get_weather_api_credential" function. Session shall have an extra feature of
    retrying for certain HTTP status code.
    :return: a Session of API.
    """
    api_config: dict | None = get_weather_api_credential()
    # setup retries for given http respones
    retires: Retry = Retry(total=3, backoff_factor=1, status_forcelist=[408, 429,500, 502, 503, 504 ])
    # Initiate API session and pass the configuration
    session: Session | None = requests.session()

    if session is not None:
        session.headers.update(**api_config)
        # mount http adapter for retries for url starting with https and http
        session.mount('https://', HTTPAdapter(max_retries=retires))
        session.mount('http://', HTTPAdapter(max_retries=retires))
        return session
    else:
        raise ValueError("Session is None")

if __name__ == "__main__":
    session = get_weather_api_session()
    # response = session.get("https://weatherapi-com.p.rapidapi.com/current.json", params={"q": 10314})

    response = session.get("http://httpstat.us/500" )
    print(response)
