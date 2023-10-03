from typing import TYPE_CHECKING, Any, Callable

from banal import ensure_list, keys_values
from pydantic import BaseModel
from runpandarun import Playbook

from investigraph.model.mapping import QueryMapping
from investigraph.settings import (
    CHUNK_SIZE,
    DEFAULT_AGGREGATOR,
    DEFAULT_EXTRACTOR,
    DEFAULT_LOADER,
    DEFAULT_SEEDER,
    DEFAULT_TRANSFORMER,
    FTM_STORE_URI,
)
from investigraph.types import TaskResult
from investigraph.util import get_func, pydantic_merge

if TYPE_CHECKING:
    from investigraph.model.context import Context

from investigraph.model.source import Source


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
        return handler(ctx, *args, **kwargs)


class SeedStage(Stage):
    _default_handler = DEFAULT_SEEDER

    glob: str | list[str] | None = None
    storage_options: dict[str, Any] = None


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

    queries: list[QueryMapping] | None = None

    def __init__(self, **data):
        data["queries"] = ensure_list(keys_values(data, "queries", "query"))
        super().__init__(**data)


class LoadStage(Stage):
    _default_handler = DEFAULT_LOADER

    index_uri: str | None = None
    fragments_uri: str | None = None
    entities_uri: str | None = None


class AggregateStage(Stage):
    _default_handler = DEFAULT_AGGREGATOR

    db_uri: str | None = FTM_STORE_URI

    def __init__(self, **data):
        if data.pop("handler", None) == "db":
            data["handler"] = "investigraph.logic.aggregate.in_db"
        super().__init__(**data)
