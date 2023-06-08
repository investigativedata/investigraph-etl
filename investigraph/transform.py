"""
Transform stage
"""

from typing import Any, Generator

from followthemoney.mapping import QueryMapping
from nomenklatura.entity import CE

from investigraph.model import Context
from investigraph.util import uplevel_proxy


def map_record(
    record: dict[str, Any], mapping: QueryMapping
) -> Generator[CE, None, None]:
    if mapping.source.check_filters(record):
        entities = mapping.map(record)
        for proxy in entities.values():
            proxy = uplevel_proxy(proxy)
            yield proxy


def map_ftm(ctx: Context, data: dict[str, Any]) -> Generator[CE, None, None]:
    for mapping in ctx.config.mappings:
        yield from map_record(data, mapping)
