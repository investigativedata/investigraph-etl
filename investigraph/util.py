from functools import cache
from pathlib import Path
from typing import Any

from banal import ensure_dict
from followthemoney import model
from nomenklatura.entity import CE, CompositeEntity


def lowercase_dict(data: Any) -> dict:
    return {str(k).lower(): v for k, v in ensure_dict(data).items()}


def make_proxy(schema: str) -> CE:
    return CompositeEntity.from_dict(model, {"schema": schema})


def ensure_proxy(data: dict[str, Any] | CE) -> CE:
    if isinstance(data, CE):
        return data
    return CompositeEntity.from_dict(model, data)


@cache
def ensure_path(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path
