import os
from pathlib import Path
from typing import Any

from banal import as_bool

from investigraph import __version__


def get_env(env: str, default: Any | None = None) -> Any | None:
    return os.environ.get(env, default)


DEBUG = as_bool(get_env("DEBUG", 1))
DATA_ROOT = Path(get_env("DATA_ROOT", Path.cwd() / "data")).absolute()
DATASETS_REPO = get_env(
    "DATASETS_REPO", "https://github.com/investigativedata/investigraph-datasets.git"
)
DATASETS_BLOCK = get_env("DATASETS_BLOCK", "github/investigraph-datasets")

DEFAULT_TRANSFORMER = get_env(
    "DEFAULT_TRANSFORMER", "investigraph.logic.transform:map_ftm"
)

REDIS_URL = get_env("REDIS_URL", "redis://localhost:6379")
CACHE_PREFIX = get_env("CACHE_PREFIX", f"investigraph:{__version__}")

FETCH_RETRIES = int(get_env("FETCH_RETRIES", 3))
FETCH_RETRY_DELAY = int(get_env("FETCH_RETRY_DELAY", 5))
