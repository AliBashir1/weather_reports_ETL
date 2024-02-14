from sqlalchemy.exc import IntegrityError, DBAPIError
from sqlalchemy.engine.base import Engine
from project_files.src.weather_reports_etl.utilities.log import log
from pandas import DataFrame


def load_weather_reports_tbl(weather_reports_df: DataFrame = None, eng: Engine = None ) -> None:
    """Loads weather_reports_df to weather_reports_table"""
    if weather_reports_df is not None:
        try:
            with eng.connect() as con:
                weather_reports_df.to_sql("weather_reports", con=con, if_exists="append", index=False)

        except (Exception, DBAPIError, IntegrityError) as e:
            raise Exception(str(e))
    else:
        raise AttributeError("Attribute reference or assignment failed.")



