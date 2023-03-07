from requests.exceptions import HTTPError
from utilities.log import log
from pandas import Series
from requests import Session


@log
def fetch_weather_reports(zipcodes_series: Series = None, session: Session = None) -> list:
    """Takes a Series of zipcodes, and HTTP requests Session to make API get call for each zipcode, returns a list of
     dictionary containing weather_reports."""
    URL = "https://weatherapi-com.p.rapidapi.com/current.json"
    weather_reports = []
    count = 0

    if ~zipcodes_series.empty and session is not None:
        # Api takes 5 digits, some of zipcodes are less than 5, add leading 0 to it
        zipcodes_series = zipcodes_series.astype(str).str.zfill(5)

        for _, zipcode in zipcodes_series.items():
            try:
                response = session.get(URL, params={"q": zipcode})
                response.raise_for_status()

                data = response.json()
                data["zipcode"] = zipcode
                weather_reports.append(data)
                # todo delete following code
                # temporary limit api calls.
                count += 1
                if count > 100:
                    break

            except HTTPError as exc:
                status_code = exc.response.status_code
                raise str(exc) + str(status_code) + str(zipcode)
        else:
            raise AttributeError("Attribute reference or assignment failed.")

    if weather_reports:
        print(type(weather_reports))
        return weather_reports


if __name__ == "__main__":
    import time
    from etl_processes.fetch_data.fetch_data_db import fetch_most_populated_zipcodes
    from connections.api_connection import get_weather_api_session
    session = get_weather_api_session()
    zipcodes_series = fetch_most_populated_zipcodes()
    start_time = time.time()
    a = fetch_weather_reports(zipcodes_series, session)
    print("fetch_weather_reports time -- {}".format(time.time() - start_time))

    print(a)
