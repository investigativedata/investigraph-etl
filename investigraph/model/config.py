import os
from functools import cache
from pathlib import Path
from typing import Any

import yaml
from nomenklatura.dataset.catalog import DataCatalog
from nomenklatura.dataset.dataset import Dataset
from nomenklatura.util import PathLike
from pydantic import BaseModel
from smart_open import open

from investigraph.exceptions import ImproperlyConfigured
from investigraph.logging import get_logger
from investigraph.settings import DATASETS_BLOCK
from investigraph.util import is_module

from .block import get_block
from .stage import ExtractStage, LoadStage, TransformStage

log = get_logger(__name__)


def abs(base: Path, path: str) -> str:
    return os.path.normpath(str(Path(base / path).absolute()))


class Config(BaseModel):
    dataset: str
    base_path: Path
    metadata: dict[str, Any]

    extract: ExtractStage
    transform: TransformStage | None = TransformStage()
    load: LoadStage | None = LoadStage()

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        # ensure absolute file paths for local sources
        super().__init__(**data)
        for source in self.extract.sources:
            if source.is_local and source.uri.startswith("."):
                source.uri = (self.base_path / source.uri).absolute()

    @classmethod
    def from_path(cls, fp: PathLike) -> "Config":
        base_path = Path(fp).parent.absolute()
        catalog = DataCatalog(Dataset, {})
        with open(fp) as fh:
            data = yaml.safe_load(fh)
        data["title"] = data.get("title", data["name"].title())
        dataset: Dataset = catalog.make_dataset(data)

        config = {
            "dataset": dataset.name,
            "base_path": base_path,
            "metadata": dataset.to_dict(),
        }

        for key in cls.__fields__:
            if key not in config and key in data:
                config[key] = data[key]

        config = cls(**config)

        # custom user code
        if not is_module(config.extract.handler):
            config.extract.handler = abs(config.base_path, config.extract.handler)
        if not is_module(config.transform.handler):
            config.transform.handler = abs(config.base_path, config.transform.handler)
        if not is_module(config.load.handler):
            config.load.handler = abs(config.base_path, config.load.handler)

        return config


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
