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
from investigraph.settings import DATASETS_DIR, DEFAULT_TRANSFORMER
from investigraph.util import ensure_pythonpath

from .block import get_block
from .source import Source


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

    class Config:
        arbitrary_types_allowed = True

    @property
    def target(self) -> str:
        if self.entities_uri is not None:
            if self.entities_uri.startswith("post"):
                return "postgres"
        return "json"

    @property
    def parse_module_path(self) -> str:
        if len(self.mappings):
            return DEFAULT_TRANSFORMER
        return f"{self.base_path.parent.name}.{self.dataset}.parse:parse"

    @classmethod
    def from_path(cls, fp: PathLike) -> "Config":
        base_path = Path(fp).parent
        ensure_pythonpath(base_path.parent.parent)
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
            base_path=base_path,
            metadata=dataset.to_dict(),
            pipeline=data["pipeline"],
            mappings=mappings,
        )


@cache
def get_config(
    dataset: str | None = None, block: str | None = None, path: PathLike | None = None
) -> Config:
    """
    Return configuration based on block or path (path has precedence)
    """
    if path is not None:
        return Config.from_path(path)
    if block is not None and dataset is not None:
        block = get_block(block)
        block.load(dataset)
        return Config.from_path(DATASETS_DIR / dataset / "config.yml")
    raise ImproperlyConfigured("Specify `dataset` and `block` or `path` to config.")
