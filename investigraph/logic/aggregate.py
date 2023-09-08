"""
aggregate fragments
"""

from typing import TYPE_CHECKING, Literal, TypeAlias
from uuid import uuid4

from followthemoney import model
from ftmq.io import smart_read_proxies
from ftmq.model.coverage import Collector
from ftmstore import get_dataset

if TYPE_CHECKING:
    from investigraph.model import Context

from ftmq.model import Coverage

from investigraph.types import CE, CEGenerator

COMMON_SCHEMAS = ("Organization", "LegalEntity")

AggregatorResult: TypeAlias = tuple[int, Coverage]


def merge(ctx: "Context", p1: CE, p2: CE) -> CE:
    try:
        p1 = p1.merge(p2)
        p1.schema = model.common_schema(p1.schema, p2.schema)
        return p1
    except Exception as e:
        # try common schemata, this will probably "downgrade" entities
        # as in, losing some schema specific properties
        for schema in COMMON_SCHEMAS:
            if p1.schema.is_a(schema) and p2.schema.is_a(schema):
                p1 = ctx.make(schema, **p1.to_dict()["properties"])
                p1.id = p2.id
                p2 = ctx.make(schema, **p2.to_dict()["properties"])
                p1 = p1.merge(p2)
                return p1

        ctx.log.warn(f"{e}, id: `{p1.id}`")
        return p1


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
                buffer[proxy.id] = merge(self.ctx, buffer[proxy.id], proxy)
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
    with ctx.config.dataset.coverage as collector:
        proxies = aggregator.iterate(collector, "memory")
    ctx.load_entities(proxies, serialize=True)
    return aggregator.fragments, collector.export()


def in_db(ctx: "Context", fragment_uris: list[str]) -> AggregatorResult:
    aggregator = Aggregator(ctx, fragment_uris)
    with ctx.config.dataset.coverage as collector:
        proxies = aggregator.iterate(collector, "db")
    ctx.load_entities(proxies, serialize=True)
    return aggregator.fragments, collector.export()
