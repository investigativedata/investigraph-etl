from datetime import datetime
from logging import Logger, LoggerAdapter
from typing import Generator, Iterable

from anystore.io import smart_write
from followthemoney.util import make_entity_id
from ftmq.util import join_slug
from nomenklatura.entity import CE, CompositeEntity
from prefect import get_run_logger
from prefect.exceptions import MissingContextError
from pydantic import BaseModel, ConfigDict

from investigraph.cache import Cache, get_cache
from investigraph.exceptions import DataError
from investigraph.logic.aggregate import AggregatorResult, merge
from investigraph.model.config import Config
from investigraph.model.source import Source
from investigraph.types import CEGenerator
from investigraph.util import make_proxy


class BaseContext(BaseModel):
    dataset: str
    prefix: str
    config: Config

    def __hash__(self) -> int:
        return hash(repr(self.model_dump()))

    def __eq__(self, other) -> bool:
        return hash(self) == hash(other)

    @property
    def cache(self) -> Cache:
        return get_cache()

    @property
    def log(self) -> LoggerAdapter | Logger:
        try:
            return get_run_logger()
        except MissingContextError:
            import logging

            return logging.getLogger(__name__)

    def load_fragments(self, *args, **kwargs) -> str:
        kwargs["uri"] = kwargs.pop("uri", self.config.load.fragments_uri)
        kwargs["parts"] = True
        return self.config.load.handle(self, *args, **kwargs)

    def load_entities(self, *args, **kwargs) -> str:
        kwargs["uri"] = kwargs.pop("uri", self.config.load.entities_uri)
        kwargs["parts"] = False
        return self.config.load.handle(self, *args, **kwargs)

    def aggregate(self, *args, **kwargs) -> AggregatorResult:
        return self.config.aggregate.handle(*args, **kwargs)

    def export_metadata(self) -> None:
        data = self.config.dataset
        data.updated_at = data.updated_at or datetime.utcnow()
        data = data.model_dump_json().encode()
        smart_write(self.config.load.index_uri, data)

    def make_proxy(self, *args, **kwargs) -> CE:
        return make_proxy(*args, dataset=self.dataset, **kwargs)

    def make(self, *args, **kwargs) -> CE:
        # align with zavod api for easy migration
        return self.make_proxy(*args, **kwargs)

    def make_slug(self, *args, **kwargs) -> str:
        prefix = kwargs.pop("prefix", self.prefix)
        return join_slug(*args, prefix=prefix, **kwargs)

    def make_id(self, *args, **kwargs) -> str:
        prefix = kwargs.pop("prefix", self.prefix)
        return join_slug(make_entity_id(*args), prefix=prefix)

    def make_cache_key(self, *args: Iterable[str]) -> str:
        return join_slug(*args, sep="#")

    def task(self) -> "TaskContext":
        return TaskContext(**self.model_dump())

    def emit(self, proxy: CE) -> None:
        raise NotImplementedError

    def from_source(self, source: Source) -> "Context":
        return Context(
            dataset=self.config.dataset.name,
            prefix=self.config.dataset.prefix,
            config=self.config,
            source=source,
        )

    def from_sources(self) -> Generator["Context", None, None]:
        for source in self.config.seed.handle(self):
            yield self.from_source(source)
        for source in self.config.extract.sources:
            yield self.from_source(source)

    @classmethod
    def from_config(cls, config: Config) -> "BaseContext":
        return cls(
            dataset=config.dataset.name,
            prefix=config.dataset.prefix,
            config=config,
        )


class Context(BaseContext):
    source: Source


class TaskContext(Context):
    proxies: dict[str, CompositeEntity] = {}
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __iter__(self) -> CEGenerator:
        yield from self.proxies.values()

    def emit(self, proxy: CE) -> None:
        if not proxy.id:
            raise DataError("No Entity ID!")
        # mimic zavod api, do merge already
        if proxy.id in self.proxies:
            self.proxies[proxy.id] = merge(self, self.proxies[proxy.id], proxy)
        else:
            self.proxies[proxy.id] = proxy


def init_context(config: Config, source: Source) -> Context:
    return Context(
        dataset=config.dataset.name,
        prefix=config.dataset.prefix,
        config=config,
        source=source,
    )
