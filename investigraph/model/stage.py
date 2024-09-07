from typing import TYPE_CHECKING, Any, Callable, ClassVar

from banal import ensure_list, keys_values
from pydantic import BaseModel
from runpandarun import Playbook

from investigraph.model.mapping import QueryMapping
from investigraph.settings import SETTINGS
from investigraph.util import get_func, pydantic_merge

if TYPE_CHECKING:
    from investigraph.model.context import Context

from investigraph.model.source import Source


class Stage(BaseModel):
    default_handler: ClassVar[str | None] = None

    handler: str
    chunk_size: int = SETTINGS.chunk_size

    def __init__(self, **data):
        data["handler"] = data.pop("handler", self.default_handler)
        super().__init__(**data)

    def get_handler(self) -> Callable:
        return get_func(self.handler)

    def handle(self, ctx: "Context", *args, **kwargs) -> Any:
        handler = self.get_handler()
        return handler(ctx, *args, **kwargs)


class SeedStage(Stage):
    default_handler: ClassVar[str] = SETTINGS.default_seeder

    glob: str | list[str] | None = None
    storage_options: dict[str, Any] | None = None
    source_options: dict[str, Any] | None = None


class ExtractStage(Stage):
    default_handler: ClassVar[str] = SETTINGS.default_extractor

    fetch: bool = True
    sources: list[Source] = []
    pandas: Playbook = Playbook()

    def __init__(self, **data):
        super().__init__(**data)
        for source in self.sources:
            source.pandas = pydantic_merge(self.pandas, source.pandas)


class TransformStage(Stage):
    default_handler: ClassVar[str] = SETTINGS.default_transformer

    queries: list[QueryMapping] = []

    def __init__(self, **data):
        data["queries"] = ensure_list(keys_values(data, "queries", "query"))
        super().__init__(**data)


class LoadStage(Stage):
    default_handler: ClassVar[str] = SETTINGS.default_loader

    uri: str = "memory:///"


class AggregateStage(Stage):
    default_handler: ClassVar[str] = SETTINGS.default_aggregator

    index_uri: str | None = None
    entities_uri: str | None = None
