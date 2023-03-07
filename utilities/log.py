import functools
import logging
import os
from datetime import date
from config.definitions import ROOT_DIR


def get_logger():
    FILENAME_INFO = f"{str(date.today())}.log"
    ERROR_FILE = f"{str(date.today())}_errors.log"

    LOG_FILE_PATH = os.path.join(ROOT_DIR, "log", FILENAME_INFO)
    ERROR_FILE_PATH = os.path.join(ROOT_DIR, "log/error_log", ERROR_FILE)
    # message and date formatter
    FORMATTER = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s", "%Y-%m-%d %H:%M:%S")


    # handlers
    file_handler = logging.FileHandler(LOG_FILE_PATH)
    stream_handler = logging.StreamHandler()
    error_file_handler = logging.FileHandler(ERROR_FILE_PATH)

    # message format
    file_handler.setFormatter(FORMATTER)
    stream_handler.setFormatter(FORMATTER)
    error_file_handler.setFormatter(FORMATTER)

    # message level
    file_handler.setLevel(logging.INFO)
    stream_handler.setLevel(logging.DEBUG)
    error_file_handler.setLevel(logging.DEBUG)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Add handler in logger
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.addHandler(error_file_handler)

    return logger


logger = get_logger()


def log(func):
    @functools.wraps(func)
    def log_wrapper(*args, **kwargs):
        logger.info(f":{func.__name__} is initiated.")
        try:
            result = func(*args, **kwargs)
            if result is None:
                logger.info(f":{func.__name__}: completed.")
            else:
                logger.info(f":{func.__name__}: completed with results {type(result)}.")
            return result
        except Exception as e:
            logger.debug(f":{func.__name__}:message: {str(e)}")

    return log_wrapper


