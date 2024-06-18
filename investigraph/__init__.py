from followthemoney.proxy import EntityProxy

from investigraph.logic.aggregate import proxy_merge
from investigraph.model import Catalog, Context, Dataset, Resolver, Source, TaskContext
from investigraph.settings import VERSION as __version__

# FIXME overwrite legacy merge with our downgrading merge for ftmstore:
EntityProxy.merge = proxy_merge

__all__ = [
    "__version__",
    "Catalog",
    "Context",
    "Dataset",
    "TaskContext",
    "Source",
    "Resolver",
]
