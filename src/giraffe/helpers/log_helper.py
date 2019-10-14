import os
import sys
import logging
from logging.handlers import RotatingFileHandler

from giraffe.exceptions.technical import TechnicalError


def get_logger(logger_name: str, storage_folder: str = '.', level: int = logging.DEBUG) -> logging.Logger:
    # Validate logger-request
    if not os.path.isdir(storage_folder):
        raise TechnicalError(f'{storage_folder} folder does not exist')
    file_name = os.path.join(storage_folder, 'giraffe_logs', logger_name + '.log')
    os.makedirs(os.path.dirname(file_name), exist_ok=True)  # Create log-file path if not exists

    # Set-up logging format
    date_time_format = "%d-%m-%Y %H:%M:%S"
    log_row_format = "[%(asctime)s] %(processName)s %(levelname)s: %(message)s"

    log_row_format = logging.Formatter(fmt=log_row_format, datefmt=date_time_format)

    # Add log-handlers
    console_handler = logging.StreamHandler(sys.stdout)
    file_handler = RotatingFileHandler(filename=file_name,
                                       mode='a',
                                       maxBytes=1_000_000,
                                       backupCount=3,
                                       encoding='utf-8',
                                       delay=False)

    file_handler.setFormatter(log_row_format)
    console_handler.setFormatter(log_row_format)

    log = logging.Logger(name=logger_name, level=level)

    log.addHandler(console_handler)
    log.addHandler(file_handler)

    log.setLevel(logging.DEBUG)

    log.debug(f'Initiating logger [name={logger_name}] in {file_name}')
    return log
