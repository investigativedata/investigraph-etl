import hashlib
import os
import re
from functools import cache
from importlib import import_module
from importlib.util import module_from_spec, spec_from_file_location
from io import BytesIO
from pathlib import Path
from typing import Any, Callable

import orjson
from banal import clean_dict, ensure_dict, ensure_list, is_listish, is_mapping
from followthemoney.util import join_text as _join_text
from ftmq.util import clean_name, make_dataset
from nomenklatura.dataset import DefaultDataset
from nomenklatura.entity import CE, CompositeEntity
from normality import slugify
from pydantic import BaseModel
from runpandarun.util import PathLike

from investigraph.exceptions import ImproperlyConfigured
from investigraph.types import SDict


def slugified_dict(data: dict[Any, Any]) -> SDict:
    return {slugify(k, "_"): v for k, v in ensure_dict(data).items()}


def make_proxy(schema: str, dataset: str | None = DefaultDataset, **properties) -> CE:
    data = {"schema": schema, "properties": properties}
    if isinstance(dataset, str):
        dataset = make_dataset(dataset)
    return CompositeEntity.from_data(dataset, data)


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


def str_or_none(value: Any) -> str | None:
    if not value:
        return None
    value = str(value).strip()
    return value or None


def join_text(*parts: Any, sep: str = " ") -> str | None:
    parts = [clean_name(p) for p in parts]
    return _join_text(*parts, sep=sep)


def checksum(io: BytesIO, algorithm: str | None = "md5") -> str:
    handler = getattr(hashlib, algorithm)
    hash_ = handler()
    for chunk in iter(lambda: io.read(128 * hash_.block_size), b""):
        hash_.update(chunk)
    return hash_.hexdigest()


def data_checksum(data: Any, algorithm: str | None = "md5") -> str:
    if is_listish(data):
        data = sorted(data, key=lambda x: repr(x))
    data = orjson.dumps(data, option=orjson.OPT_SORT_KEYS, default=str)
    return checksum(BytesIO(data), algorithm)


def is_empty(value: Any) -> bool:
    if isinstance(value, (bool, int)):
        return False
    if value == "":
        return False
    return not value


def dict_merge(d1: dict[Any, Any], d2: dict[Any, Any]) -> dict[Any, Any]:
    # update d1 with d2 but omit empty values
    d1, d2 = clean_dict(d1), clean_dict(d2)
    for key, value in d2.items():
        if not is_empty(value):
            if is_mapping(value):
                value = ensure_dict(value)
                d1[key] = dict_merge(d1.get(key, {}), value)
            elif is_listish(value):
                d1[key] = ensure_list(d1.get(key)) + ensure_list(value)
            else:
                d1[key] = value
    return d1


def pydantic_merge(m1: BaseModel, m2: BaseModel) -> BaseModel:
    if m1.__class__ != m2.__class__:
        raise ImproperlyConfigured(
            f"Cannot merge: `{m1.__class__.__name__}` with `{m2.__class__.__name__}`"
        )
    return m1.__class__(**dict_merge(m1.dict(), m2.dict()))


def to_dict(obj: Any) -> dict[Any]:
    if hasattr(obj, "dict"):
        return obj.dict()
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    return ensure_dict(obj)
