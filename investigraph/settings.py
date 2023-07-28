import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from banal import as_bool


def get_env(env: str, default: Any | None = None) -> Any | None:
    return os.environ.get(env, default)


VERSION = "0.3.0"
RUN_TIME = datetime.utcnow().replace(microsecond=0)

DEBUG = as_bool(get_env("DEBUG", 1))
DATA_ROOT = Path(get_env("DATA_ROOT", Path.cwd() / "data")).absolute()
DATASETS_REPO = "https://github.com/investigativedata/investigraph-datasets.git"
DATASETS_BLOCK = get_env("DATASETS_BLOCK")

DEFAULT_EXTRACTOR = get_env("DEFAULT_EXTRACTOR", "investigraph.logic.extract:handle")
DEFAULT_TRANSFORMER = get_env(
    "DEFAULT_TRANSFORMER", "investigraph.logic.transform:map_ftm"
)
DEFAULT_LOADER = get_env("DEFAULT_LOADER", "investigraph.logic.load:load_proxies")

REDIS_URL = get_env("REDIS_URL", "redis://localhost:6379")
REDIS_PREFIX = get_env("REDIS_PREFIX", f"investigraph:{VERSION}")

TASK_CACHE = as_bool(get_env("TASK_CACHE", 1))
TASK_RETRIES = int(get_env("TASK_RETRIES", 3))
TASK_RETRY_DELAY = int(get_env("TASK_RETRY_DELAY", 5))
TASK_CACHE_EXPIRATION = int(get_env("TASK_CACHE_EXPIRATION", 0)) or None  # in minutes
TASK_CACHE_EXPIRATION = (
    timedelta(TASK_CACHE_EXPIRATION) if TASK_CACHE_EXPIRATION is not None else None
)

TASK_RUNNER = get_env("PREFECT_TASK_RUNNER", "").lower()

CHUNK_SIZE = int(get_env("CHUNK_SIZE", 1000))
