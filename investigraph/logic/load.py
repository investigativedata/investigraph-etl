"""
Load transformed data (aka CE proxies) to various targets like json files,
databases, s3 endpoints...
"""

from typing import Any, Iterable

from ftmstore import get_dataset

from investigraph.util import smart_write_proxies


def to_fragments(uri: str, proxies: Iterable[dict[str, Any]]):
    smart_write_proxies(uri, proxies, "ab")


def to_store(uri: str, dataset: str, proxies: Iterable[dict[str, Any]]):
    dataset = get_dataset(dataset, database_uri=uri)
    bulk = dataset.bulk()
    for ix, proxy in enumerate(proxies):
        bulk.put(proxy, fragment=str(ix))
    bulk.flush()
