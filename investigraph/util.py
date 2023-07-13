import sys
from functools import cache
from importlib import import_module, invalidate_caches
from pathlib import Path
from typing import Any, Callable

from banal import ensure_dict
from followthemoney import model
from followthemoney.proxy import E
from followthemoney.util import sanitize_text
from nomenklatura.entity import CE, CompositeEntity
from normality import slugify

from investigraph.types import SDict


def slugified_dict(data: dict[Any, Any]) -> SDict:
    return {slugify(k, "_"): v for k, v in ensure_dict(data).items()}


def make_proxy(schema: str) -> CE:
    return CompositeEntity.from_dict(model, {"schema": schema})


def uplevel_proxy(proxy: E) -> CE:
    return CompositeEntity.from_dict(model, proxy.to_dict())


@cache
def ensure_path(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path.absolute()


@cache
def ensure_pythonpath(path: Path) -> None:
    path = str(path)
    if path not in sys.path:
        sys.path.append(path)


@cache
def get_func(path: str) -> Callable:
    # FIXME regex
    if "/" not in path:
        invalidate_caches()
        module, func = path.split(":")
        module = import_module(module)
        return getattr(module, func)
    raise NotImplementedError(path)


def clean_string(value: Any) -> str | None:
    """
    Convert a value to None or a sanitized string without linebreaks
    """
    value = sanitize_text(value)
    if value is None:
        return
    return " ".join(value.split())
