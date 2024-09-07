"""
Load transformed data (proxy fragments) to a nomenklatura statement store
"""

from typing import TYPE_CHECKING, Iterable, TypeAlias

from ftmq.util import make_proxy

from investigraph.types import SDict

if TYPE_CHECKING:
    from investigraph.model import Context

TProxies: TypeAlias = Iterable[SDict]


def handle(ctx: "Context", proxies: TProxies) -> int:
    ix = 0
    with ctx.store.writer() as bulk:
        for proxy in proxies:
            bulk.add_entity(make_proxy(proxy))
            ix += 1
    return ix
