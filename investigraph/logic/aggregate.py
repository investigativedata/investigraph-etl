"""
aggregate fragments
"""

import logging
from uuid import uuid4

from ftmstore import get_dataset

from investigraph.model import Context
from investigraph.util import smart_iter_proxies

log = logging.getLogger(__name__)


def in_memory(ctx: Context, in_uri: str) -> tuple[int, int]:
    """
    aggregate in memory: read fragments from `in_uri` and write aggregated
    proxies to `out_uri`

    as `smart_open` is used here, `in_uri` and `out_uri` can be any local or
    remote locations
    """
    fragments = 0
    buffer = {}
    for proxy in smart_iter_proxies(in_uri):
        fragments += 1
        if proxy.id in buffer:
            buffer[proxy.id].merge(proxy)
        else:
            buffer[proxy.id] = proxy

    ctx.entities_loader.write(buffer.values(), serialize=True)
    return fragments, len(buffer.values())


def in_db(ctx: Context, in_uri: str) -> tuple[int, int]:
    """
    use ftm store database to aggregate.
    `in_uri`: database connection string
    """
    dataset = get_dataset("aggregate_%s" % uuid4().hex)
    bulk = dataset.bulk()
    for ix, proxy in enumerate(smart_iter_proxies(in_uri)):
        if ix % 10000 == 0:
            log.info("Write [%s]: %s entities", dataset.name, ix)
        bulk.put(proxy, fragment=str(ix))
    bulk.flush()
    proxies = []
    for ox, proxy in enumerate(dataset.iterate()):
        proxies.append(proxy)
        if ox % 10000 == 0:
            ctx.entities_loader.write(proxies, serialize=True)
            proxies = []
    ctx.entities_loader.write(proxies, serialize=True)
    dataset.drop()
    return ix, ox
