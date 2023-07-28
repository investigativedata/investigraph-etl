from datetime import datetime
from typing import Iterable

import orjson
from followthemoney.util import make_entity_id
from nomenklatura.entity import CE
from nomenklatura.util import datetime_iso
from prefect import get_run_logger
from prefect.logging.loggers import PrefectLogAdapter
from pydantic import BaseModel
from smart_open import open

from investigraph.cache import Cache, get_cache
from investigraph.model.config import Config
from investigraph.model.source import Source
from investigraph.settings import DATA_ROOT
from investigraph.types import CEGenerator
from investigraph.util import ensure_path, join_slug, make_proxy


class Context(BaseModel):
    dataset: str
    prefix: str
    config: Config
    source: Source

    class Config:
        arbitrary_types_allowed = True

    def __hash__(self) -> int:
        return hash((self.dataset, self.source.uri))

    @property
    def cache(self) -> Cache:
        return get_cache()

    @property
    def log(self) -> PrefectLogAdapter:
        return get_run_logger()

    def load_fragments(self, *args, **kwargs) -> str:
        kwargs["uri"] = kwargs.pop("uri", self.config.load.fragments_uri)
        kwargs["parts"] = True
        return self.config.load.handle(self, *args, **kwargs)

    def load_entities(self, *args, **kwargs) -> str:
        kwargs["uri"] = kwargs.pop("uri", self.config.load.entities_uri)
        kwargs["parts"] = False
        return self.config.load.handle(self, *args, **kwargs)

    def export_metadata(self) -> None:
        data = self.config.dataset
        data.updated_at = data.updated_at or datetime_iso(datetime.utcnow())
        data = orjson.dumps(data.dict())
        with open(self.config.load.index_uri, "wb") as fh:
            fh.write(data)

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
        return TaskContext(**self.dict())

    def emit(self) -> None:
        raise NotImplementedError


class TaskContext(Context):
    proxies: list[CE] = []

    def __iter__(self) -> CEGenerator:
        yield from self.proxies

    def emit(self, proxy: CE) -> None:
        # mimic zavod
        self.proxies.append(proxy)


def init_context(config: Config, source: Source) -> Context:
    path = ensure_path(DATA_ROOT / config.dataset.name)
    if config.load.index_uri is None:
        config.load.index_uri = (path / "index.json").as_uri()
    if config.load.fragments_uri is None:
        config.load.fragments_uri = (path / "fragments.json").as_uri()
    if config.load.entities_uri is None:
        config.load.entities_uri = (path / "entities.ftm.json").as_uri()

    return Context(
        dataset=config.dataset.name,
        prefix=config.dataset.prefix,
        config=config,
        source=source,
    )
