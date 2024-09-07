"""
Inspect dataset pipelines interactively
"""

from typing import Any, Generator, Iterable

import pandas as pd
from nomenklatura.entity import CE

from investigraph.logging import get_logger
from investigraph.logic.extract import extract_records_from_source
from investigraph.logic.transform import transform_record
from investigraph.model.config import Config, get_config
from investigraph.model.context import BaseContext, Context
from investigraph.util import PathLike

log = get_logger(__name__)


def log_error(msg: str):
    log.error(f"[bold red]ERROR[/bold red] {msg}")


def get_records(ctx: Context, limit: int | None = 5) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    log.info("Extracting `%s` ..." % ctx.source.uri)
    for record in extract_records_from_source(ctx):
        records.append(record)
        if len(records) == limit:
            return records
    return records


def inspect_config(p: PathLike) -> Config:
    config = get_config(p)
    try:
        if not callable(config.extract.get_handler()):
            log_error(f"module not found or not callable: `{config.extract.handler}`")
    except ModuleNotFoundError:
        log_error(f"no custom extract module: `{config.extract.handler}`")
    try:
        if not callable(config.transform.get_handler()):
            log_error(f"module not found or not callable: `{config.transform.handler}`")
    except ModuleNotFoundError:
        log_error(f"no custom transform module: `{config.transform.handler}`")
    try:
        if not callable(config.load.get_handler()):
            log_error(f"module not found or not callable: `{config.load.handler}`")
    except ModuleNotFoundError:
        log_error(f"no custom load module: `{config.load.handler}`")
    return config


def inspect_seed(config: Config, limit: int | None = 5) -> pd.DataFrame:
    """
    Preview seeded sources in tabular format
    """
    ctx = BaseContext.from_config(config)
    sources = []
    for ix, sctx in enumerate(ctx.from_sources(), 1):
        sources.append(sctx.source.model_dump())
        if ix == limit:
            break
    return pd.DataFrame(sources)


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
            proxies.extend(transform_record(sctx, rec, ix))
        yield sctx.source.name, proxies
