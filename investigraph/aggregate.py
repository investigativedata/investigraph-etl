"""
aggregate fragments
"""

from uuid import uuid4

from ftmstore import get_dataset
from ftmstore.cli import iterate_stream, write_stream
from smart_open import open

from investigraph.util import smart_iter_proxies, smart_write_proxies


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
    infile = open(in_uri)
    outfile = open(out_uri)
    write_stream(dataset, infile)
    iterate_stream(dataset, outfile)
    dataset.drop()
    return 0, 0  # FIXME
