import hashlib
import os
import re
from functools import cache
from importlib import import_module
from importlib.util import module_from_spec, spec_from_file_location
from io import BytesIO
from pathlib import Path
from typing import Any, Callable

from banal import clean_dict, ensure_dict
from followthemoney import model
from followthemoney.proxy import E
from followthemoney.util import sanitize_text
from nomenklatura.entity import CE, CompositeEntity
from normality import slugify
from pydantic import BaseModel
from runpandarun.util import PathLike

from .exceptions import ImproperlyConfigured
from .types import SDict


def slugified_dict(data: dict[Any, Any]) -> SDict:
    return {slugify(k, "_"): v for k, v in ensure_dict(data).items()}


def make_proxy(schema: str) -> CE:
    return CompositeEntity.from_dict(model, {"schema": schema})


def uplevel_proxy(proxy: E) -> CE:
    return CompositeEntity.from_dict(model, proxy.to_dict())


@cache
def ensure_path(path: PathLike) -> Path:
    path = Path(os.path.normpath(path))
    path.mkdir(parents=True, exist_ok=True)
    return path.absolute()


module_re = re.compile(r"^[\w\.]+:[\w]+")


@cache
def is_module(path: str) -> bool:
    return bool(module_re.match(path))


@cache
def get_func(path: str) -> Callable:
    module, func = path.rsplit(":", 1)
    if is_module(path):
        module = import_module(module)
    else:
        path = Path(module)
        spec = spec_from_file_location(path.stem, module)
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
    return getattr(module, func)


def clean_string(value: Any) -> str | None:
    """
    Convert a value to None or a sanitized string without linebreaks
    """
    value = sanitize_text(value)
    if value is None:
        return
    return " ".join(value.split())


def checksum(io: BytesIO, algorithm: str | None = "md5") -> str:
    handler = getattr(hashlib, algorithm)
    hash_ = handler()
    for chunk in iter(lambda: io.read(128 * hash_.block_size), b""):
        hash_.update(chunk)
    return hash_.hexdigest()


def is_empty(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    return not value


def dict_merge(d1: dict[Any, Any], d2: dict[Any, Any]) -> dict[Any, Any]:
    # update d1 with d2 but omit empty values
    d1, d2 = clean_dict(d1), clean_dict(d2)
    for key, value in d2.items():
        if not is_empty(value):
            if isinstance(value, dict):
                d1[key] = dict_merge(d1.get(key, {}), value)
            else:
                d1[key] = value
    return d1


def pydantic_merge(m1: BaseModel, m2: BaseModel) -> BaseModel:
    if m1.__class__ != m2.__class__:
        raise ImproperlyConfigured(
            f"Cannot merge: `{m1.__class__.__name__}` with `{m2.__class__.__name__}`"
        )
    return m1.__class__(**dict_merge(m1.dict(), m2.dict()))
