"""
aggregate fragments
"""

from typing import TYPE_CHECKING

from followthemoney.proxy import E
from ftmq.aggregate import merge
from ftmq.io import smart_write_proxies
from ftmq.model.coverage import Collector, DatasetStats
from ftmq.types import CE
from ftmq.util import make_proxy

if TYPE_CHECKING:
    from investigraph.model import Context

from investigraph.types import CEGenerator


def proxy_merge(self: E, other: E) -> CE:
    """
    Used to override `EntityProxy.merge` in `investigraph.__init__.py`
    """
    return merge(
        make_proxy(self.to_dict()), make_proxy(other.to_dict()), downgrade=True
    )


def get_iterator(proxies: CEGenerator, collector: Collector) -> CEGenerator:
    for proxy in proxies:
        collector.collect(proxy)
        yield proxy


def handle(ctx: "Context") -> DatasetStats:
    collector = Collector()
    proxies = ctx.store.iterate(dataset=ctx.dataset)
    iterator = get_iterator(proxies, collector)
    smart_write_proxies(ctx.config.aggregate.entities_uri, iterator, serialize=True)
    return collector.export()
