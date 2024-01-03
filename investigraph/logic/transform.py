"""
Transform stage: map data records to ftm proxies
"""

from typing import TYPE_CHECKING

from investigraph.model.mapping import QueryMapping

if TYPE_CHECKING:
    from investigraph.model import Context

from ftmq.util import make_proxy

from investigraph.types import CEGenerator, SDict


def map_record(
    record: SDict, mapping: QueryMapping, dataset: str | None = "default"
) -> CEGenerator:
    mapping = mapping.get_mapping()
    if mapping.source.check_filters(record):
        entities = mapping.map(record)
        for proxy in entities.values():
            yield make_proxy(proxy.to_dict(), dataset=dataset)


def map_ftm(ctx: "Context", data: SDict, ix: int) -> CEGenerator:
    for mapping in ctx.config.transform.queries:
        yield from map_record(data, mapping, ctx.config.dataset.name)
