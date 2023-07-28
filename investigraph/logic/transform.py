"""
Transform stage: map data records to ftm proxies
"""

from typing import TYPE_CHECKING

from investigraph.model.mapping import QueryMapping

if TYPE_CHECKING:
    from investigraph.model import Context

from investigraph.types import CEGenerator, SDict
from investigraph.util import uplevel_proxy


def map_record(record: SDict, mapping: QueryMapping) -> CEGenerator:
    mapping = mapping.get_mapping()
    if mapping.source.check_filters(record):
        entities = mapping.map(record)
        for proxy in entities.values():
            proxy = uplevel_proxy(proxy)
            yield proxy


def map_ftm(ctx: "Context", data: SDict, ix: int) -> CEGenerator:
    for mapping in ctx.config.transform.queries:
        yield from map_record(data, mapping)
