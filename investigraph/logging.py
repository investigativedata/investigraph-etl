import logging
import sys
from logging import Filter, LogRecord
from typing import Any, Dict, List

import structlog
from prefect import get_run_logger
from prefect.exceptions import MissingContextError
from structlog.contextvars import merge_contextvars
from structlog.dev import ConsoleRenderer, set_exc_info
from structlog.processors import (
    JSONRenderer,
    TimeStamper,
    UnicodeDecoder,
    add_log_level,
    format_exc_info,
)
from structlog.stdlib import (
    BoundLogger,
    LoggerFactory,
    ProcessorFormatter,
    add_logger_name,
)
from structlog.stdlib import get_logger as get_raw_logger

from investigraph.settings import SETTINGS


def get_logger(name: str, *args, **kwargs):
    try:
        return get_run_logger()
    except MissingContextError:
        return get_raw_logger(name, *args, **kwargs)


def configure_logging(level: int = logging.INFO) -> None:
    """Configure log levels and structured logging"""
    shared_processors: List[Any] = [
        add_log_level,
        add_logger_name,
        # structlog.stdlib.PositionalArgumentsFormatter(),
        # structlog.processors.StackInfoRenderer(),
        merge_contextvars,
        set_exc_info,
        TimeStamper(fmt="iso"),
        # format_exc_info,
        UnicodeDecoder(),
    ]

    if SETTINGS.log_json:
        shared_processors.append(format_exc_info)
        shared_processors.append(format_json)
        formatter = ProcessorFormatter(
            foreign_pre_chain=shared_processors,
            processor=JSONRenderer(),
        )
    else:
        formatter = ProcessorFormatter(
            foreign_pre_chain=shared_processors,
            processor=ConsoleRenderer(
                exception_formatter=structlog.dev.plain_traceback
            ),
        )

    processors = shared_processors + [
        ProcessorFormatter.wrap_for_formatter,
    ]

    # configuration for structlog based loggers
    structlog.configure(
        cache_logger_on_first_use=True,
        # wrapper_class=AsyncBoundLogger,
        wrapper_class=BoundLogger,
        processors=processors,
        context_class=dict,
        logger_factory=LoggerFactory(),
    )

    # handler for low level logs that should be sent to STDERR
    out_handler = logging.StreamHandler(sys.stderr)
    out_handler.setLevel(level)
    out_handler.addFilter(_MaxLevelFilter(logging.WARNING))
    out_handler.setFormatter(formatter)
    # handler for high level logs that should be sent to STDERR
    error_handler = logging.StreamHandler(sys.stderr)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(SETTINGS.log_level.upper())
    root_logger.addHandler(out_handler)
    root_logger.addHandler(error_handler)

    logging.getLogger("httpx").setLevel(logging.WARNING)


def format_json(_: Any, __: Any, ed: Dict[str, str]) -> Dict[str, str]:
    """Stackdriver uses `message` and `severity` keys to display logs"""
    ed["message"] = ed.pop("event")
    ed["severity"] = ed.pop("level", "info").upper()
    return ed


class _MaxLevelFilter(Filter):
    def __init__(self, highest_log_level: int) -> None:
        self._highest_log_level = highest_log_level

    def filter(self, log_record: LogRecord) -> bool:
        return log_record.levelno <= self._highest_log_level
