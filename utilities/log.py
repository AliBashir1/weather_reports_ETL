import functools
import logging
import os
from datetime import date
from config.definitions import ROOT_DIR
from pandas import DataFrame, Series


def _get_logger():
    LOG_FILE = f"{str(date.today())}_errors.log"
    LOG_FILE_PATH = os.path.join(ROOT_DIR, "log/error_log", LOG_FILE)
    # message and date formatter
    FORMATTER = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s", "%Y-%m-%d %H:%M:%S")


    # handlers
    Log_file_handler = logging.FileHandler(LOG_FILE_PATH)
    stream_handler = logging.StreamHandler()


    # message format
    Log_file_handler.setFormatter(FORMATTER)
    stream_handler.setFormatter(FORMATTER)

    # message level
    stream_handler.setLevel(logging.DEBUG)
    Log_file_handler.setLevel(logging.DEBUG)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Add handler in logger
    logger.addHandler(stream_handler)
    logger.addHandler(Log_file_handler)

    return logger


logger = _get_logger()


def log(func):
    @functools.wraps(func)
    def log_wrapper(*args, **kwargs):
        logger.info(":{}::{}".format(func.__name__, func.__doc__))
        try:
            result = func(*args, **kwargs)
            temp = result
            if type(temp) == DataFrame or Series or list or tuple:
                temp = type(temp)
            if temp is None:
                logger.info(":{}::Completed.".format(func.__name__))
            else:
                logger.info(":{}::Completed with results {}.".format(func.__name__, temp))
            return result
        except Exception as e:
            logger.debug(":{}::message::{}".format(func.__name__, str(e)))
    return log_wrapper


if __name__ == "__main__":
    @log
    def afunc():
        return 3+5


    @log
    def afunc1():
        return 6 + 5


    @log
    def afunc2():
        return 7 + 5

    afunc()