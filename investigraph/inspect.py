"""
Inspect dataset pipelines interactively
"""


from typing import Any, Generator

import pandas as pd
from nomenklatura.entity import CE
from nomenklatura.util import PathLike
from rich import print

from investigraph.logic.extract import iter_records
from investigraph.logic.fetch import fetch_source
from investigraph.model.config import Config, get_config
from investigraph.model.context import init_context
from investigraph.model.source import Source
from investigraph.util import get_func


def print_error(msg: str):
    print(f"[bold red]ERROR[/bold red] {msg}")


def get_records(source: Source) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    res = fetch_source(source)
    for ix, rec in enumerate(iter_records(res)):
        records.append(rec)
        if ix == 5:
            return records


def inspect_config(p: PathLike) -> Config:
    config = get_config(path=p)
    try:
        func = get_func(config.parse_module_path)
        if not callable(func):
            print_error(
                f"module not found or not callable: `{config.parse_module_path}`"
            )
    except ModuleNotFoundError:
        print_error(f"no parsing function: `{config.parse_module_path}`")
    return config


def inspect_extract(config: Config) -> Generator[tuple[str, str], None, None]:
    """
    Preview fetched & extracted records in tabular format
    """
    for source in config.pipeline.sources:
        df = pd.DataFrame(get_records(source))
        yield source.name, df.to_markdown(index=False)


def inspect_transform(config: Config) -> Generator[tuple[str, CE], None, None]:
    """
    Preview first proxies
    """
    func = get_func(config.parse_module_path)
    for source in config.pipeline.sources:
        ctx = init_context(config, source)
        proxies: list[CE] = []
        for rec in get_records(source):
            for proxy in func(ctx, rec):
                proxies.append(proxy)
        yield source.name, proxies
