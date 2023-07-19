"""
Inspect dataset pipelines interactively
"""


from typing import Any, Generator

import pandas as pd
from nomenklatura.entity import CE
from rich import print

from investigraph.logic.extract import extract_pandas
from investigraph.model import Resolver, Source
from investigraph.model.config import Config, get_config
from investigraph.model.context import init_context
from investigraph.util import PathLike


def print_error(msg: str):
    print(f"[bold red]ERROR[/bold red] {msg}")


def get_records(source: Source) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    print("Fetching `%s` ..." % source.uri)
    res = Resolver(source=source)
    for ix, rec in enumerate(extract_pandas(res)):
        records.append(rec)
        if ix == 5:
            return records


def inspect_config(p: PathLike) -> Config:
    config = get_config(path=p)
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


def inspect_extract(config: Config) -> Generator[tuple[str, pd.DataFrame], None, None]:
    """
    Preview fetched & extracted records in tabular format
    """
    for source in config.extract.sources:
        df = pd.DataFrame(get_records(source))
        yield source.name, df


def inspect_transform(config: Config) -> Generator[tuple[str, CE], None, None]:
    """
    Preview first proxies
    """
    for source in config.extract.sources:
        ctx = init_context(config, source)
        proxies: list[CE] = []
        for ix, rec in enumerate(get_records(source)):
            for proxy in ctx.config.transform.handle(ctx, rec, ix):
                proxies.append(proxy)
        yield source.name, proxies
