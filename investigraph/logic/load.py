"""
Load transformed data (aka CE proxies) to various targets like json files,
databases, s3 endpoints...
"""

from functools import cache
from typing import TYPE_CHECKING, Iterable, TypeAlias
from urllib.parse import urlparse

from ftmq.io import smart_write_proxies
from ftmstore import get_dataset

from investigraph.types import SDict

if TYPE_CHECKING:
    from investigraph.model import Context

TProxies: TypeAlias = Iterable[SDict]


def to_uri(uri: str, proxies: TProxies, **kwargs):
    smart_write_proxies(uri, proxies, **kwargs)


def to_store(uri: str, dataset: str, proxies: TProxies):
    dataset = get_dataset(dataset, database_uri=uri)
    bulk = dataset.bulk()
    for ix, proxy in enumerate(proxies):
        bulk.put(proxy, fragment=str(ix))
    bulk.flush()


class Loader:
    """
    write entity proxies to anywhere
    """

    def __init__(self, ctx: "Context", uri: str, parts: bool | None = False):
        self.uri = uri
        self.ctx = ctx
        self.is_store = "sql" in urlparse(uri).scheme
        self.parts = parts

    def write(self, proxies: TProxies, **kwargs) -> None:
        if self.is_store:
            to_store(self.uri, self.ctx.dataset, proxies)
            return self.uri
        else:
            uri = self.uri
            if self.parts:
                uri += f".{kwargs.pop('ckey')}"
            to_uri(uri, proxies, **kwargs)
            return uri


@cache
def get_loader(ctx: "Context", uri: str, parts: bool | None = False) -> Loader:
    return Loader(ctx, uri, parts)


def load_proxies(
    ctx: "Context", proxies: TProxies, uri: str, parts: bool | None = False, **kwargs
) -> str:
    loader = get_loader(ctx, uri, parts)
    return loader.write(proxies, **kwargs)
