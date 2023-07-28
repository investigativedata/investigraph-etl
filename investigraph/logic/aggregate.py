"""
aggregate fragments
"""

from uuid import uuid4

from ftmq.io import smart_read_proxies
from ftmstore import get_dataset

from investigraph.model import Context
from investigraph.settings import CHUNK_SIZE
from investigraph.types import CE

COMMON_SCHEMAS = ("Organization", "LegalEntity")


def merge(ctx: Context, p1: CE, p2: CE) -> CE:
    try:
        p1.merge(p2)
        return p1
    except Exception as e:
        # try common schemata, this will probably "downgrade" entities
        # as in, losing some schema specific properties
        for schema in COMMON_SCHEMAS:
            if p1.schema.is_a(schema) and p2.schema.is_a(schema):
                p1 = ctx.make(schema, **p1.to_dict()["properties"])
                p1.id = p2.id
                p2 = ctx.make(schema, **p2.to_dict()["properties"])
                p1.merge(p2)
                return p1

        ctx.log.warn(f"{e}, id: `{p1.id}`")


def in_memory(ctx: Context, *uris: tuple[str]) -> tuple[int, int]:
    """
    aggregate in memory: read fragments from `in_uri` and write aggregated
    proxies to `out_uri`

    as `smart_open` is used here, `in_uri` and `out_uri` can be any local or
    remote locations
    """
    ix = 0
    buffer = {}
    for ix, proxy in enumerate(smart_read_proxies(uris), 1):
        if ix % (CHUNK_SIZE * 10) == 0:
            ctx.log.info("reading in proxy %d ..." % ix)
        if proxy.id in buffer:
            buffer[proxy.id] = merge(ctx, buffer[proxy.id], proxy)
        else:
            buffer[proxy.id] = proxy

    ctx.load_entities(buffer.values(), serialize=True)
    return ix, len(buffer.values())


def in_db(ctx: Context, in_uri: str) -> tuple[int, int]:
    """
    use ftm store database to aggregate.
    `in_uri`: database connection string
    """
    dataset = get_dataset("aggregate_%s" % uuid4().hex)
    bulk = dataset.bulk()
    for ix, proxy in enumerate(smart_read_proxies(in_uri)):
        if ix % 10_000 == 0:
            ctx.log.info("Write [%s]: %s entities", dataset.name, ix)
        bulk.put(proxy, fragment=str(ix))
    bulk.flush()
    proxies = []
    for ox, proxy in enumerate(dataset.iterate()):
        proxies.append(proxy)
        if ox % 10_000 == 0:
            ctx.load_entities(proxies, serialize=True)
            proxies = []
    ctx.load_entities(proxies, serialize=True)
    dataset.drop()
    return ix, ox
