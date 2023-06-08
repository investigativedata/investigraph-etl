import sys
from functools import cache
from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Generator, Iterable

import orjson
from banal import ensure_dict
from followthemoney import model
from followthemoney.proxy import E
from nomenklatura.entity import CE, CompositeEntity
from smart_open import open


def lowercase_dict(data: Any) -> dict:
    return {str(k).lower(): v for k, v in ensure_dict(data).items()}


def make_proxy(schema: str) -> CE:
    return CompositeEntity.from_dict(model, {"schema": schema})


def uplevel_proxy(proxy: E) -> CE:
    return CompositeEntity.from_dict(model, proxy.to_dict())


def load_proxy(data: dict[str, Any]) -> CE:
    return CompositeEntity.from_dict(model, data)


def ensure_proxy(data: dict[str, Any] | CE) -> CE:
    if isinstance(data, CE):
        return data
    return load_proxy(data)


def smart_iter_proxies(uri: str) -> Generator[CE, None, None]:
    for line in open(uri):
        yield load_proxy(orjson.loads(line))


def smart_write_proxies(
    uri: str,
    proxies: Iterable[CE | dict[str, Any]],
    mode: str | None = "wb",
    serialize: bool | None = False,
) -> int:
    with open(uri, mode) as f:
        for ix, proxy in enumerate(proxies):
            if serialize:
                proxy = proxy.to_dict()
            f.write(orjson.dumps(proxy, option=orjson.OPT_APPEND_NEWLINE))
    return ix + 1


@cache
def ensure_path(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


@cache
def ensure_pythonpath(path: Path) -> None:
    path = str(path)
    if path not in sys.path:
        sys.path.append(path)


@cache
def get_func(parse_module_path: str) -> Callable:
    module, func = parse_module_path.split(":")
    module = import_module(module)
    return getattr(module, func)
