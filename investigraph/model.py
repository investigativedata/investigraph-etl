from datetime import datetime
from functools import cache
from importlib import import_module
from typing import Any, Callable

import requests
import yaml
from dateparser import parse as parse_date
from nomenklatura.dataset.catalog import DataCatalog
from nomenklatura.dataset.dataset import Dataset
from nomenklatura.util import PathLike
from pantomime import normalize_mimetype
from pydantic import BaseModel

from investigraph.cache import Cache, get_cache
from investigraph.settings import DATASETS_DIR, DATASETS_MODULE
from investigraph.util import lowercase_dict


class Source(BaseModel):
    name: str
    uri: str
    extract_kwargs: dict | None = {}


class SourceHead(BaseModel):
    source: Source
    etag: str | None = None
    last_modified: datetime | None = None
    content_type: str | None = None

    def __init__(self, **data):
        content_type = data.pop("content-type", None)
        last_modified = data.pop("last-modified", "")
        super().__init__(
            last_modified=parse_date(last_modified),
            content_type=normalize_mimetype(content_type),
            **data,
        )

    @classmethod
    def from_source(cls, source: Source) -> "SourceHead":
        res = requests.head(source.uri)
        return cls(source=source, **lowercase_dict(res.headers))


class SourceResult(Source):
    header: dict
    content: bytes

    @property
    def mimetype(self) -> str:
        return normalize_mimetype(self.header["content-type"])


class Pipeline(BaseModel):
    sources: list[Source]


class Config(BaseModel):
    dataset: str
    metadata: dict[str, Any]
    pipeline: Pipeline
    fragments_uri: str | None = None
    entities_uri: str | None = None

    @property
    def parse_module_path(self) -> str:
        return f"{DATASETS_MODULE}.{self.dataset}.parse"

    @classmethod
    def from_path(cls, fp: PathLike) -> "Config":
        catalog = DataCatalog(Dataset, {})
        with open(fp, "r") as fh:
            data = yaml.safe_load(fh)
        dataset: Dataset = catalog.make_dataset(data)
        return cls(
            dataset=dataset.name, metadata=dataset.to_dict(), pipeline=data["pipeline"]
        )


class Context(BaseModel):
    dataset: str
    config: Config
    source: Source
    run_id: str

    @property
    def cache(self) -> Cache:
        return get_cache()


@cache
def get_config(dataset: str) -> Config:
    return Config.from_path(DATASETS_DIR / dataset / "config.yml")


@cache
def get_parse_func(parse_module_path: str) -> Callable:
    module = import_module(parse_module_path)
    return getattr(module, "parse")
