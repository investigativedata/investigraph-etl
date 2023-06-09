"""
Load transformed data (aka CE proxies) to various targets like json files,
databases, s3 endpoints...
"""

from functools import lru_cache
from typing import Iterable, TypeAlias
from urllib.parse import urlparse

from ftmstore import get_dataset

from investigraph.types import SDict
from investigraph.util import smart_write_proxies

Proxies: TypeAlias = Iterable[SDict]


def to_uri(uri: str, proxies: Proxies, **kwargs):
    smart_write_proxies(uri, proxies, "ab", **kwargs)


def to_store(uri: str, dataset: str, proxies: Proxies):
    dataset = get_dataset(dataset, database_uri=uri)
    bulk = dataset.bulk()
    for ix, proxy in enumerate(proxies):
        bulk.put(proxy, fragment=str(ix))
    bulk.flush()


class Loader:
    """
    write entity proxies to anywhere
    """

    def __init__(self, uri: str, dataset: str):
        self.uri = uri
        self.dataset = dataset
        parsed = urlparse(uri)
        self.is_store = "sql" in parsed.scheme

    def write(self, proxies: Proxies, **kwargs) -> None:
        if self.is_store:
            to_store(self.uri, self.dataset, proxies)
        else:
            to_uri(self.uri, proxies, **kwargs)


@lru_cache(1024)
def get_loader(uri: str, dataset: str) -> Loader:
    return Loader(uri, dataset)
