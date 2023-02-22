import requests
from configparser import ConfigParser
from config.definitions import ROOT_DIR
import os


def get_weather_api_session():
    """
    This function returns a session object for api calls.
    :return: a session object
    """
    config_file_path = os.path.join(ROOT_DIR, "config", "conf.ini")
    config = ConfigParser()
    config.read(config_file_path)

    # get api configuration from config file. it should be a x-rapidapi-host and x-rapidapi-key in dict type
    api_config = dict(config["API"])

    # Intiate API session and pass the configuration
    session = requests.session()
    session.headers.update(**api_config)

    return session


if __name__ == "__main__":
    session = get_weather_api_session()
    response = session.get("https://weatherapi-com.p.rapidapi.com/current.json", params={"q": 10314})
    print(response.json())