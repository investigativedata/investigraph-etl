"""
Inspect dataset pipelines interactively
"""

from typing import Any, Generator, Iterable

import pandas as pd
from nomenklatura.entity import CE
from rich import print

from investigraph.model import Resolver
from investigraph.model.config import Config, get_config
from investigraph.model.context import BaseContext, Context
from investigraph.util import PathLike


def print_error(msg: str):
    print(f"[bold red]ERROR[/bold red] {msg}")


def get_records(ctx: Context, limit: int | None = 5) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    # print("Extracting `%s` ..." % ctx.source.uri)
    res = Resolver(source=ctx.source)
    if res.source.is_http and ctx.config.extract.fetch:
        res._resolve_http()
    for rec in ctx.config.extract.handle(ctx, res):
        records.append(rec)
        if len(records) == limit:
            return records
    return records


def inspect_config(p: PathLike) -> Config:
    config = get_config(p)
    try:
        if not callable(config.extract.get_handler()):
            print_error(f"module not found or not callable: `{config.extract.handler}`")
    except ModuleNotFoundError:
        print_error(f"no custom extract module: `{config.extract.handler}`")
    try:
        if not callable(config.transform.get_handler()):
            print_error(
                f"module not found or not callable: `{config.transform.handler}`"
            )
    except ModuleNotFoundError:
        print_error(f"no custom transform module: `{config.transform.handler}`")
    try:
        if not callable(config.load.get_handler()):
            print_error(f"module not found or not callable: `{config.load.handler}`")
    except ModuleNotFoundError:
        print_error(f"no custom load module: `{config.load.handler}`")
    return config


def inspect_extract(
    config: Config, limit: int | None = 5
) -> Generator[tuple[str, pd.DataFrame], None, None]:
    """
    Preview fetched & extracted records in tabular format
    """
    ctx = BaseContext.from_config(config)
    for ix, sctx in enumerate(ctx.from_sources(), 1):
        df = pd.DataFrame(get_records(sctx, limit))
        yield sctx.source.name, df
        if ix == limit:
            return


def inspect_transform(
    config: Config, limit: int | None = 5
) -> Generator[tuple[str, Iterable[CE]], None, None]:
    """
    Preview first proxies
    """
    ctx = BaseContext.from_config(config)
    for ix, sctx in enumerate(ctx.from_sources(), 1):
        proxies: list[CE] = []
        for ix, rec in enumerate(get_records(sctx, limit)):
            for proxy in sctx.config.transform.handle(sctx, rec, ix):
                proxies.append(proxy)
        yield sctx.source.name, proxies
