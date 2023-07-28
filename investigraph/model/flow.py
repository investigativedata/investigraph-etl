from typing import Any

from normality import slugify
from pydantic import BaseModel, root_validator

from investigraph.model.config import Config, get_config
from investigraph.settings import CHUNK_SIZE, DATASETS_BLOCK


class FlowOptions(BaseModel):
    dataset: str | None = None
    block: str | None = None
    config: str | None = None
    aggregate: bool | None = None
    chunk_size: int | None = CHUNK_SIZE

    index_uri: str | None = None
    fragments_uri: str | None = None
    entities_uri: str | None = None

    @property
    def flow_name(self) -> str:
        if self.dataset is not None:
            return self.dataset
        return slugify(self.config)

    @root_validator
    def validate_options(cls, values):
        block = values.get("dataset") and (values.get("block") or DATASETS_BLOCK)
        config = values.get("config")
        if not block and not config:
            raise ValueError("Specify at least a config file or a block and dataset")
        return values


class Flow(BaseModel):
    dataset: str
    config: Config

    def __init__(self, **data):
        # override base config with runtime options
        options = data.pop("options")
        config = get_config(data.pop("dataset", None), options.block, options.config)

        self.assign(config.extract, "chunk_size", options.chunk_size)
        self.assign(config.transform, "chunk_size", options.chunk_size)
        self.assign(config.load, "chunk_size", options.chunk_size)

        self.assign(config.load, "index_uri", options.index_uri)
        self.assign(config.load, "fragments_uri", options.fragments_uri)
        self.assign(config.load, "entities_uri", options.entities_uri)
        self.assign(config.load, "aggregate", options.aggregate)

        super().__init__(dataset=config.dataset.name, config=config, **data)

    @property
    def should_aggregate(self) -> bool:
        if self.config.load.entities_uri.startswith("postg"):
            return False
        return self.config.load.aggregate

    @classmethod
    def from_options(cls, options: FlowOptions) -> "Flow":
        return cls(dataset=options.dataset, options=options)

    @staticmethod
    def assign(base: Any, attr: str, value: Any) -> None:
        if value is not None:
            setattr(base, attr, value)
