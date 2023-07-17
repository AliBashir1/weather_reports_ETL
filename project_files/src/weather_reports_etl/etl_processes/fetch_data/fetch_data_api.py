from requests.exceptions import HTTPError
from src.weather_reports_etl.utilities.log import log
from pandas import Series


@log
def fetch_weather_reports(*, zipcodes: Series = None) -> list:
    """Takes a Series of zipcodes, and HTTP requests Session to make API get call for each zipcode, returns a list of
     dictionary containing weather_reports."""
    URL = "https://weatherapi-com.p.rapidapi.com/current.json"
    weather_reports = []
    count = 0
    session = get_weather_api_session()

    if zipcodes is not None and session is not None:
        # Api takes 5 digits, some of zipcodes are less than 5, add leading 0 to it
        zipcodes = zipcodes.astype(str).str.zfill(5)

        for _, zipcode in zipcodes.items():
            try:
                response = session.get(URL, params={"q": zipcode})
                response.raise_for_status()

                data = response.json()
                data["zipcode"] = zipcode
                weather_reports.append(data)
                # todo delete following code
                # temporary limit api calls.
                count += 1
                if count > 2:
                    break

            except HTTPError as exc:
                status_code = exc.response.status_code
                raise str(exc) + str(status_code) + str(zipcode)
    else:
        raise AttributeError("Attribute reference or assignment failed.")

    if weather_reports:
        return weather_reports


if __name__ == "__main__":
    import time
    from project_files.src.weather_reports_etl.etl_processes.fetch_data.fetch_data_db import fetch_most_populated_zipcodes
    from project_files.src.weather_reports_etl.connections.api_connection import get_weather_api_session

    start_time = time.time()
    zipcodes_series = fetch_most_populated_zipcodes()
    a = fetch_weather_reports(zipcodes=zipcodes_series)
    print("fetch_weather_reports time -- {}".format(time.time() - start_time))

    print(a)
