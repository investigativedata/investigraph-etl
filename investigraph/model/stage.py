from typing import TYPE_CHECKING, Any, Callable

from banal import keys_values
from followthemoney.mapping import QueryMapping
from pydantic import BaseModel
from runpandarun import Playbook

from investigraph.logic.load import TProxies
from investigraph.logic.transform import load_mappings
from investigraph.settings import (
    CHUNK_SIZE,
    DEFAULT_EXTRACTOR,
    DEFAULT_LOADER,
    DEFAULT_TRANSFORMER,
)
from investigraph.types import TaskResult
from investigraph.util import get_func, pydantic_merge

if TYPE_CHECKING:
    from .context import Context

from .source import Source


class Stage(BaseModel):
    _default_handler = None

    handler: str
    chunk_size: int | None = CHUNK_SIZE

    def __init__(self, **data):
        data["handler"] = data.pop("handler", self._default_handler)
        super().__init__(**data)

    def get_handler(self) -> Callable:
        return get_func(self.handler)

    def handle(self, ctx: "Context", *args, **kwargs) -> TaskResult:
        handler = self.get_handler()
        yield from handler(ctx, *args, **kwargs)


class ExtractStage(Stage):
    _default_handler = DEFAULT_EXTRACTOR

    fetch: bool | None = True
    sources: list[Source] | None = []
    pandas: Playbook | None = Playbook()

    def __init__(self, **data):
        super().__init__(**data)
        for source in self.sources:
            source.pandas = pydantic_merge(self.pandas, source.pandas)


class TransformStage(Stage):
    _default_handler = DEFAULT_TRANSFORMER

    query: list[dict[str, Any]] | None = None
    mappings: list[QueryMapping] | None = None

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        data["query"] = keys_values(data, "queries", "query")
        data["mappings"] = load_mappings(tuple(data["query"]))
        super().__init__(**data)


class LoadStage(Stage):
    _default_handler = DEFAULT_LOADER

    index_uri: str | None = None
    fragments_uri: str | None = None
    entities_uri: str | None = None
    aggregate: bool | None = True

    @property
    def target(self) -> str:
        if self.entities_uri is not None:
            if self.entities_uri.startswith("postg"):
                return "postgres"
        return "json"

    def handle(self, ctx: "Context", proxies: TProxies, *args, **kwargs) -> str:
        handler = self.get_handler()
        if self.target == "postgres":
            # write directly to entities instead of fragments
            # as aggregation is happening within postgres store on write
            kwargs["uri"] = kwargs.pop("uri", self.entities_uri)
        return handler(ctx, proxies, *args, **kwargs)
