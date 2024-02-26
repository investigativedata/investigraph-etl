"""
aggregate fragments
"""

from typing import TYPE_CHECKING, Literal, TypeAlias
from uuid import uuid4

from ftmq.aggregate import merge
from ftmq.io import smart_read_proxies
from ftmq.model.coverage import Collector, DatasetStats
from ftmstore import get_dataset

if TYPE_CHECKING:
    from investigraph.model import Context

from investigraph.types import CEGenerator

COMMON_SCHEMAS = ("Organization", "LegalEntity")

AggregatorResult: TypeAlias = tuple[int, DatasetStats]


class Aggregator:
    """
    Aggregate loaded fragments to entities
    """

    def __init__(self, ctx: "Context", fragment_uris: list[str]) -> None:
        self.ctx = ctx
        self.fragment_uris = fragment_uris
        self.fragments = 0

    def get_fragments(self) -> CEGenerator:
        ix = -1
        for ix, proxy in enumerate(smart_read_proxies(self.fragment_uris)):
            if ix % self.ctx.config.aggregate.chunk_size == 0:
                self.ctx.log.info("reading in proxy %d ..." % ix)
            yield proxy
        self.fragments = ix + 1

    def aggregate_db(self) -> CEGenerator:
        dataset = get_dataset(
            "aggregate_%s" % uuid4().hex, database_uri=self.ctx.config.aggregate.db_uri
        )
        bulk = dataset.bulk()
        for ix, proxy in enumerate(self.get_fragments()):
            bulk.put(proxy, fragment=str(ix))
        bulk.flush()
        yield from dataset.iterate()
        dataset.drop()

    def aggregate_memory(self) -> CEGenerator:
        buffer = {}
        for proxy in self.get_fragments():
            if proxy.id in buffer:
                buffer[proxy.id] = merge(buffer[proxy.id], proxy)
            else:
                buffer[proxy.id] = proxy
        yield from buffer.values()

    def iterate(
        self, collector: Collector, handler: Literal["memory", "db"] | None = "memory"
    ) -> CEGenerator:
        aggregator = self.aggregate_db if handler == "db" else self.aggregate_memory
        for proxy in aggregator():
            collector.collect(proxy)
            yield proxy


def in_memory(ctx: "Context", fragment_uris: list[str]) -> AggregatorResult:
    aggregator = Aggregator(ctx, fragment_uris)
    collector = Collector()
    proxies = aggregator.iterate(collector, "memory")
    ctx.load_entities(proxies, serialize=True)
    return aggregator.fragments, collector.export()


def in_db(ctx: "Context", fragment_uris: list[str]) -> AggregatorResult:
    aggregator = Aggregator(ctx, fragment_uris)
    collector = Collector()
    proxies = aggregator.iterate(collector, "db")
    ctx.load_entities(proxies, serialize=True)
    return aggregator.fragments, collector.export()
