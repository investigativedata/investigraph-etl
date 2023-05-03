import os
from pathlib import Path
from typing import Any


def get_env(env: str, default: Any | None = None) -> Any | None:
    return os.environ.get(env, default)


DATASETS_DIR = get_env("DATASETS_DIR", "datasets")
DATASETS_MODULE = DATASETS_DIR.split("/")[-1]
DATASETS_DIR = Path.cwd() / DATASETS_DIR
DATA_ROOT = get_env("DATA_ROOT", Path.cwd() / "data")
