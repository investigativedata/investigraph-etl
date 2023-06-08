from datetime import datetime
from functools import cache
from typing import Any

import orjson
import requests
import yaml
from banal import clean_dict, keys_values
from dateparser import parse as parse_date
from followthemoney import model
from followthemoney.mapping import QueryMapping
from nomenklatura.dataset.catalog import DataCatalog
from nomenklatura.dataset.dataset import Dataset
from nomenklatura.util import PathLike, datetime_iso
from pantomime import normalize_mimetype
from pydantic import BaseModel
from smart_open import open

from investigraph.block import get_block
from investigraph.cache import Cache, get_cache
from investigraph.settings import DATASETS_DIR, DATASETS_MODULE
from investigraph.util import lowercase_dict


class Source(BaseModel):
    name: str | None = None
    uri: str
    extract_kwargs: dict | None = {}

    def __init__(self, **data):
        if not data.get("name"):
            data["name"] = data["uri"]
        super().__init__(**data)


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
    mappings: list[QueryMapping]
    pipeline: Pipeline
    index_uri: str | None = None
    fragments_uri: str | None = None
    entities_uri: str | None = None
    aggregate: bool | None = True

    class Config:
        arbitrary_types_allowed = True

    @property
    def parse_module_path(self) -> str:
        if len(self.mappings):
            return "investigraph.transform:map_ftm"
        return f"{DATASETS_MODULE}.{self.dataset}.parse:parse"

    @classmethod
    def from_path(cls, fp: PathLike) -> "Config":
        catalog = DataCatalog(Dataset, {})
        with open(fp, "r") as fh:
            data = yaml.safe_load(fh)
        dataset: Dataset = catalog.make_dataset(data)
        mappings = []
        if data.get("mapping") is not None:
            for m in keys_values(data["mapping"], "queries", "query"):
                m.pop("database", None)
                m["csv_url"] = "/dev/null"
                mapping = model.make_mapping(m)
                mappings.append(mapping)

        return cls(
            dataset=dataset.name,
            metadata=dataset.to_dict(),
            pipeline=data["pipeline"],
            mappings=mappings,
        )


class FlowOptions(BaseModel):
    dataset: str
    block: str | None = None
    config: str | None = None
    index_uri: str | None = None
    fragments_uri: str | None = None
    entities_uri: str | None = None
    aggregate: bool | None = True


class Flow(BaseModel):
    dataset: str
    config: Config

    def __init__(self, **data):
        # override base config with runtime options
        options = data.get("options")
        if options is not None:
            options = dict(options)
        config = get_config(
            data["dataset"], options.get("block"), options.get("config")
        )
        data["config"] = {**clean_dict(config.dict()), **options}
        super().__init__(**data)

    @classmethod
    def from_options(cls, options: FlowOptions) -> "Flow":
        return cls(dataset=options.dataset, options=options)


class Context(BaseModel):
    dataset: str
    prefix: str
    config: Config
    source: Source
    run_id: str

    @property
    def cache(self) -> Cache:
        return get_cache()

    def export_metadata(self) -> None:
        with open(self.config.index_uri, "wb") as fh:
            data = self.config.metadata
            data["updated_at"] = data.get("updated_at", datetime_iso(datetime.utcnow()))
            data = orjson.dumps(data)
            fh.write(data)


@cache
def get_config(
    dataset: str, block: str | None = None, path: PathLike | None = None
) -> Config:
    """
    Return configuration based on block or path (path has precedence)
    """
    if path is not None:
        return Config.from_path(path)
    if block is not None:
        block = get_block(block)
        block.load(dataset)
    return Config.from_path(DATASETS_DIR / dataset / "config.yml")
