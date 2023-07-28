import logging
from pathlib import Path

import yaml
from pydantic import BaseModel
from runpandarun.util import absolute_path

from investigraph.exceptions import ImproperlyConfigured
from investigraph.model.block import get_block
from investigraph.model.dataset import Dataset
from investigraph.model.mixins import RemoteMixin, YamlMixin
from investigraph.model.stage import ExtractStage, LoadStage, TransformStage
from investigraph.settings import DATASETS_BLOCK
from investigraph.util import PathLike, is_module

log = logging.getLogger(__name__)


class Config(BaseModel, YamlMixin, RemoteMixin):
    dataset: Dataset
    base_path: Path | None = Path()
    extract: ExtractStage | None = ExtractStage()
    transform: TransformStage | None = TransformStage()
    load: LoadStage | None = LoadStage()

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        if "dataset" not in data:
            data["dataset"] = data
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
