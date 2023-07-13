"""
Transform stage: map data records to ftm proxies
"""

from typing import TYPE_CHECKING, Any

from followthemoney import model
from followthemoney.mapping import QueryMapping

if TYPE_CHECKING:
    from investigraph.model import Context

from investigraph.types import CEGenerator, SDict
from investigraph.util import uplevel_proxy


def load_mappings(data: list[dict[str, Any]]) -> list[QueryMapping]:
    mappings = []
    for m in data:
        m.pop("database", None)
        m["csv_url"] = "/dev/null"
        mapping = model.make_mapping(m)
        mappings.append(mapping)
    return mappings


def map_record(record: SDict, mapping: QueryMapping) -> CEGenerator:
    if mapping.source.check_filters(record):
        entities = mapping.map(record)
        for proxy in entities.values():
            proxy = uplevel_proxy(proxy)
            yield proxy


def map_ftm(ctx: "Context", data: SDict, ix: int) -> CEGenerator:
    for mapping in ctx.config.transform.mappings:
        yield from map_record(data, mapping)
