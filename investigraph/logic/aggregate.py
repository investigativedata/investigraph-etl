"""
aggregate fragments
"""

from typing import TYPE_CHECKING, Literal, TypeAlias
from urllib.parse import urlparse
from uuid import uuid4

from followthemoney.proxy import E
from ftmq.aggregate import merge
from ftmq.io import smart_read_proxies
from ftmq.model.coverage import Collector, DatasetStats
from ftmq.types import CE
from ftmq.util import make_proxy
from ftmstore import get_dataset

if TYPE_CHECKING:
    from investigraph.model import Context

from investigraph.types import CEGenerator

AggregatorResult: TypeAlias = tuple[int, DatasetStats]


def proxy_merge(self: E, other: E) -> CE:
    return merge(
        make_proxy(self.to_dict()), make_proxy(other.to_dict()), downgrade=True
    )


class Aggregator:
    """
    Aggregate loaded fragments to entities
    """

    def __init__(self, ctx: "Context", fragment_uris: list[str]) -> None:
        self.ctx = ctx
        self.fragment_uris = fragment_uris
        self.fragments = 0
        self.is_ftm_store = "sql" in urlparse(self.ctx.config.load.fragments_uri).scheme

    def get_fragments(self) -> CEGenerator:
        if self.is_ftm_store:
            dataset = get_dataset(
                self.ctx.dataset, database_uri=self.ctx.config.load.fragments_uri
            )
            iterator = enumerate(dataset.iterate())
        else:
            iterator = enumerate(smart_read_proxies(self.fragment_uris))
        ix = -1
        for ix, proxy in iterator:
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
                buffer[proxy.id] = merge(buffer[proxy.id], proxy, downgrade=True)
            else:
                buffer[proxy.id] = proxy
        yield from buffer.values()

    def iterate(
        self, collector: Collector, handler: Literal["memory", "db"] | None = "memory"
    ) -> CEGenerator:
        if self.is_ftm_store:  # already aggregated
            aggregator = self.get_fragments()
        elif handler == "db":
            aggregator = self.aggregate_db()
        else:
            aggregator = self.aggregate_memory()

        for proxy in aggregator:
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
