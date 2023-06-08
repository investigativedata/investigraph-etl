import os
from pathlib import Path
from typing import Any

from banal import as_bool

from investigraph import __version__


def get_env(env: str, default: Any | None = None) -> Any | None:
    return os.environ.get(env, default)


DEBUG = as_bool(get_env("DEBUG", 1))
DATA_ROOT = get_env("DATA_ROOT", Path.cwd() / "data")
DATASETS_REPO = "https://github.com/investigativedata/investigraph-datasets.git"
DATASETS_BLOCK = "github/investigraph-datasets"
DATASETS_DIR = Path(get_env("DATASETS_DIR", DATA_ROOT / "datasets"))
DATASETS_MODULE = DATASETS_DIR.parts[-1]

REDIS_URL = get_env("REDIS_URL", "redis://localhost:6379")
CACHE_PREFIX = get_env("CACHE_PREFIX", f"investigraph:{__version__}")

FETCH_RETRIES = int(get_env("FETCH_RETRIES", 3))
FETCH_RETRY_DELAY = int(get_env("FETCH_RETRY_DELAY", 5))
