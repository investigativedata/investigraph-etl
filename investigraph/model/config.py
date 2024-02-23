import logging
from pathlib import Path
from typing import Self
from urllib.parse import urlparse

from anystore.mixins import BaseModel
from anystore.types import Uri
from ftmq.model import Dataset
from pydantic import ConfigDict
from runpandarun.util import absolute_path

from investigraph.model.stage import (
    AggregateStage,
    ExtractStage,
    LoadStage,
    SeedStage,
    TransformStage,
)
from investigraph.util import PathLike, is_module

log = logging.getLogger(__name__)


class Config(BaseModel):
    dataset: Dataset
    base_path: Path | None = Path()
    seed: SeedStage | None = SeedStage()
    extract: ExtractStage | None = ExtractStage()
    transform: TransformStage | None = TransformStage()
    load: LoadStage | None = LoadStage()
    aggregate: bool | AggregateStage | None = AggregateStage()
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, **data):
        if "dataset" not in data:
            data["dataset"] = data
        super().__init__(**data)
        # ensure absolute file paths for local sources
        self.base_path = Path(self.base_path).absolute()
        for source in self.extract.sources:
            source.ensure_uri(self.base_path)

    @classmethod
    def from_uri(cls, uri: Uri, base_path: PathLike | None = None) -> Self:
        if base_path is None:
            u = urlparse(str(uri))
            if not u.scheme or u.scheme == "file":
                base_path = Path(uri).absolute().parent
        config = cls._from_uri(uri, base_path=base_path)

        # custom user code
        if not is_module(config.seed.handler):
            config.seed.handler = str(
                absolute_path(config.seed.handler, config.base_path)
            )
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
        if config.aggregate:
            if not is_module(config.aggregate.handler):
                config.aggregate.handler = str(
                    absolute_path(config.aggregate.handler, config.base_path)
                )

        return config


def get_config(uri: Uri) -> Config:
    return Config.from_uri(uri)
