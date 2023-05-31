import os
from pathlib import Path
from typing import Any

from banal import as_bool

from investigraph import __version__


def get_env(env: str, default: Any | None = None) -> Any | None:
    return os.environ.get(env, default)


DEBUG = as_bool(get_env("DEBUG", 1))
DATASETS_DIR = get_env("DATASETS_DIR", "datasets")
DATASETS_MODULE = DATASETS_DIR.split("/")[-1]
DATASETS_DIR = Path.cwd() / DATASETS_DIR
DATA_ROOT = get_env("DATA_ROOT", Path.cwd() / "data")
REDIS_URL = get_env("REDIS_URL", "redis://localhost:6379")
CACHE_PREFIX = get_env("CACHE_PREFIX", f"investigraph:{__version__}")
