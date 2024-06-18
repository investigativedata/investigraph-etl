import logging

from prefect import get_run_logger
from prefect.exceptions import MissingContextError


def get_logger(name: str):
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(name)
