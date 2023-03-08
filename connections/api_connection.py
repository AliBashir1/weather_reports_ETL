import requests
from requests.sessions import Session
from utilities.log import log
from utilities.config_parser import get_config_parser


@log
def get_weather_api_session() -> Session:
    """ Fetch requests.session() object"""
    config = get_config_parser()
    # get api configuration from config_file file. it should be a x-rapidapi-host and x-rapidapi-key in dict type
    api_config = dict(config["API"])

    # Initiate API session and pass the configuration
    session = requests.session()
    session.headers.update(**api_config)

    return session


if __name__ == "__main__":
    session = get_weather_api_session()
    response = session.get("https://weatherapi-com.p.rapidapi.com/current.json", params={"q": 10314})

    print(response.json())