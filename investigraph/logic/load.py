"""
Load transformed data (aka CE proxies) to various targets like json files,
databases, s3 endpoints...
"""

from functools import cache
from typing import Iterable, TypeAlias
from urllib.parse import urlparse

import shortuuid
from ftmq.io import smart_write_proxies
from ftmstore import get_dataset

from investigraph.cache import get_cache
from investigraph.types import SDict

Proxies: TypeAlias = Iterable[SDict]


def to_uri(uri: str, proxies: Proxies, **kwargs):
    smart_write_proxies(uri, proxies, **kwargs)


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

    def __init__(self, uri: str, dataset: str, parts: bool | None = False):
        self.uri = uri
        self.dataset = dataset
        parsed = urlparse(uri)
        self.is_store = "sql" in parsed.scheme
        self.cache = get_cache()
        self.parts = parts

    def write(self, proxies: Proxies, **kwargs) -> None:
        if self.is_store:
            to_store(self.uri, self.dataset, proxies)
            return self.uri
        else:
            uri = self.uri
            if self.parts:
                uri += f".{shortuuid.uuid()}"
                self.cache.sadd(uri, key=self.uri)
            to_uri(uri, proxies, **kwargs)
            return uri


@cache
def get_loader(uri: str, dataset: str, parts: bool | None = False) -> Loader:
    return Loader(uri, dataset, parts)
