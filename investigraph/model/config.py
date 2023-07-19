from functools import cache
from pathlib import Path
from typing import Any

import yaml
from nomenklatura.dataset.catalog import DataCatalog
from nomenklatura.dataset.dataset import Dataset
from pydantic import BaseModel
from runpandarun.util import absolute_path
from smart_open import open

from investigraph.exceptions import ImproperlyConfigured
from investigraph.logging import get_logger
from investigraph.settings import DATASETS_BLOCK
from investigraph.types import SDict
from investigraph.util import PathLike, is_module

from .block import get_block
from .stage import ExtractStage, LoadStage, TransformStage

log = get_logger(__name__)


def make_dataset(data: dict[str, Any]) -> Dataset:
    # erf
    catalog = DataCatalog(Dataset, {})
    if "name" not in data and "dataset" in data:
        data["name"] = data["dataset"]
    data["title"] = data.get("title", data["name"].title())
    return catalog.make_dataset(data)


class Config(BaseModel):
    dataset: str
    base_path: Path | None = Path()
    metadata: SDict

    extract: ExtractStage | None = ExtractStage()
    transform: TransformStage | None = TransformStage()
    load: LoadStage | None = LoadStage()

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        if "metadata" not in data:
            dataset = make_dataset(data)
            data["dataset"] = dataset.name
            data["metadata"] = dataset.to_dict()
        super().__init__(**data)
        # ensure absolute file paths for local sources
        self.base_path = Path(self.base_path).absolute()
        for source in self.extract.sources:
            source.ensure_uri(self.base_path)

    @classmethod
    def from_string(cls, data: str, base_path: PathLike | None = ".") -> "Config":
        data = yaml.safe_load(data)
        data["base_path"] = Path(base_path)
        config = cls(**data)

        # custom user code
        if not is_module(config.extract.handler):
            config.extract.handler = str(
                absolute_path(config.extract.handler, config.base_path)
            )
        if not is_module(config.transform.handler):
            config.transform.handler = str(
                absolute_path(config.transform.handler, config.base_path)
            )
        if not is_module(config.load.handler):
            config.load.handler = str(
                absolute_path(config.load.handler, config.base_path)
            )

        return config

    @classmethod
    def from_path(cls, fp: PathLike) -> "Config":
        with open(fp) as fh:
            data = fh.read()
        return cls.from_string(data, base_path=Path(fp).parent)


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
        block = block or DATASETS_BLOCK
        if block is not None:
            block = get_block(block)
            log.info("Using block `%s`" % block)
            block.load(dataset)
            return Config.from_path(block.path / dataset / "config.yml")
    raise ImproperlyConfigured("Specify `dataset` and `block` or `path` to config.")
