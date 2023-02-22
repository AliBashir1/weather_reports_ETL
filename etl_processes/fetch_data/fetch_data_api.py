from etl_processes.fetch_data.fetch_data_db import fetch_most_populated_zipcodes
from connections.api_connection import get_weather_api_session
from requests.exceptions import HTTPError


def fetch_weather_reports():
    """
    This function fetches weather reports from weatherapi.com, using zipcodes from fetch_most_populated_zipcodes().

    :return: a list of dictionary holding weather reports.
    """
    URL = "https://weatherapi-com.p.rapidapi.com/current.json"
    weather_reports = []
    count = 0
    session = get_weather_api_session()

    # fetch_most_populated_zipcodes returns a pandas dataframe. in the method we only need zipcode series from df
    zipcodes_series = fetch_most_populated_zipcodes()

    # Api takes 5 digits, some of zipcodes are less than 5, add leading 0 to it
    zipcodes_series = zipcodes_series.astype(str).str.zfill(5)

    # a loop with run to fetch weather reports for each zipcode and append it in weather_reports.
    zipcode_errors_report = [["zipcodes", "http_error_code"]]

    for zipcode in zipcodes_series.values:
        try:
            response = session.get(URL, params={"q": zipcode})
            response.raise_for_status()

            data = response.json()
            data["zipcode"] = zipcode
            weather_reports.append(data)
            # todo delete following code
            # temporary limit api calls.
            count += 1
            if count > 15:
                break

        except HTTPError as exc:
            status_code = exc.response.status_code
            zipcode_errors_report.append([zipcode,status_code])

    with open("/Users/alibashir/Desktop/workspace.nosync/ETL/weather_reports_ETL/log/error_log/zipcode_errors.txt", "w") as file:
        file.write(str(zipcode_errors_report))

    if weather_reports:
        return weather_reports


if __name__ == "__main__":
    import time
    a = fetch_most_populated_zipcodes()
    print(a)
    start_time = time.time()
    a = fetch_weather_reports()
    print("fetch_weather_reports time -- {}".format(time.time() - start_time))

    print(a)
