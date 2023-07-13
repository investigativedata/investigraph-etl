from functools import cache
from pathlib import Path
from typing import Any

import yaml
from banal import keys_values
from followthemoney import model
from followthemoney.mapping import QueryMapping
from nomenklatura.dataset.catalog import DataCatalog
from nomenklatura.dataset.dataset import Dataset
from nomenklatura.util import PathLike
from pydantic import BaseModel
from smart_open import open

from investigraph.exceptions import ImproperlyConfigured
from investigraph.logging import get_logger
from investigraph.settings import CHUNK_SIZE, DATASETS_BLOCK, DEFAULT_TRANSFORMER
from investigraph.util import ensure_pythonpath

from .block import get_block
from .source import Source

log = get_logger(__name__)


class Pipeline(BaseModel):
    sources: list[Source]


class Config(BaseModel):
    dataset: str
    base_path: Path
    metadata: dict[str, Any]
    mappings: list[QueryMapping]
    pipeline: Pipeline
    index_uri: str | None = None
    fragments_uri: str | None = None
    entities_uri: str | None = None
    aggregate: bool | None = True
    chunk_size: int | None = CHUNK_SIZE

    class Config:
        arbitrary_types_allowed = True

    @property
    def target(self) -> str:
        if self.entities_uri is not None:
            if self.entities_uri.startswith("postg"):
                return "postgres"
        return "json"

    @property
    def parse_module_path(self) -> str:
        if len(self.mappings):
            return DEFAULT_TRANSFORMER
        return f"{self.base_path.parent.name}.{self.dataset}.parse:parse"

    @classmethod
    def from_path(cls, fp: PathLike) -> "Config":
        base_path = Path(fp).parent.absolute()
        ensure_pythonpath(base_path.parent.parent)
        catalog = DataCatalog(Dataset, {})
        with open(fp) as fh:
            data = yaml.safe_load(fh)
        data["title"] = data.get("title", data["name"].title())
        dataset: Dataset = catalog.make_dataset(data)
        mappings = []
        if data.get("mapping") is not None:
            for m in keys_values(data["mapping"], "queries", "query"):
                m.pop("database", None)
                m["csv_url"] = "/dev/null"
                mapping = model.make_mapping(m)
                mappings.append(mapping)

        config = {
            "dataset": dataset.name,
            "base_path": base_path,
            "metadata": dataset.to_dict(),
            "mappings": mappings,
        }

        for key in cls.__fields__:
            if key not in config:
                config[key] = data.get(key)

        return cls(**config)


@cache
def get_config(
    dataset: str | None = None, block: str | None = None, path: PathLike | None = None
) -> Config:
    """
    Return configuration based on block or path (path has precedence)
    """
    if path is not None:
        return Config.from_path(path)
    if dataset is not None:
        if block is not None:
            block = get_block(block)
        else:
            block = get_block(DATASETS_BLOCK)
        log.info("Using block `%s`" % block)
        block.load(dataset)
        return Config.from_path(block.path / dataset / "config.yml")
    raise ImproperlyConfigured("Specify `dataset` and `block` or `path` to config.")
