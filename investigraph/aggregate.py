"""
aggregate fragments
"""

import logging
from uuid import uuid4

from ftmstore import get_dataset

from investigraph.util import smart_iter_proxies, smart_write_proxies

log = logging.getLogger(__name__)


def in_memory(in_uri: str, out_uri: str) -> tuple[int, int]:
    fragments = 0
    buffer = {}
    for proxy in smart_iter_proxies(in_uri):
        fragments += 1
        if proxy.id in buffer:
            buffer[proxy.id].merge(proxy)
        else:
            buffer[proxy.id] = proxy
    proxies = smart_write_proxies(out_uri, buffer.values(), serialize=True)
    return fragments, proxies


def in_db(in_uri: str, out_uri: str) -> tuple[int, int]:
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
            smart_write_proxies(out_uri, proxies, serialize=True)
            proxies = []
    smart_write_proxies(out_uri, proxies, serialize=True)
    dataset.drop()
    return ix, ox
