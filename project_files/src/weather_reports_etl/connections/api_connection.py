import requests
from requests.sessions import Session
from src.weather_reports_etl.utilities.config_parser import get_config_parser
from src.weather_reports_etl.utilities.encryptions import __decrypt

def get_weather_api_session() -> Session:
    """ Fetch requests.session() object"""
    config = get_config_parser()
    # get api host and encrypted key
    host = config.get("API", "x-rapidapi-host")
    api_key = __decrypt(config.get("API", "x-rapidapi-key"))
    print(api_key)
    api_config = {
        "x-rapidapi-host": host,
        "x-rapidapi-key": api_key

    }

    # Initiate API session and pass the configuration
    session = requests.session()
    session.headers.update(**api_config)

    return session


if __name__ == "__main__":
    session = get_weather_api_session()
    response = session.get("https://weatherapi-com.p.rapidapi.com/current.json", params={"q": 10314})

    print(response.json())