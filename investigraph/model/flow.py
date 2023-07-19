from banal import as_bool
from pydantic import BaseModel, root_validator

from investigraph.settings import CHUNK_SIZE, DATASETS_BLOCK

from .config import Config, get_config


class FlowOptions(BaseModel):
    dataset: str | None = None
    block: str | None = None
    config: str | None = None
    aggregate: bool | None = None
    chunk_size: int | None = CHUNK_SIZE

    index_uri: str | None = None
    fragments_uri: str | None = None
    entities_uri: str | None = None

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
        options = data.pop("options", None)
        if options is not None:
            options = dict(options)
        config = get_config(
            data.pop("dataset", None), options.get("block"), options.get("config")
        )
        if "chunk_size" in options:
            chunk_size = options["chunk_size"]
            if chunk_size is not None:
                config.extract.chunk_size = chunk_size
                config.transform.chunk_size = chunk_size
                config.load.chunk_size = chunk_size
        config.load.index_uri = options.get("index_uri") or config.load.index_uri
        config.load.fragments_uri = (
            options.get("fragments_uri") or config.load.fragments_uri
        )
        config.load.entities_uri = (
            options.get("entities_uri") or config.load.entities_uri
        )
        config.load.aggregate = (
            as_bool(options.get("aggregate")) or config.load.aggregate
        )

        super().__init__(dataset=config.dataset, config=config, **data)

    @property
    def should_aggregate(self) -> bool:
        if self.config.load.entities_uri.startswith("postg"):
            return False
        return self.config.load.aggregate

    @classmethod
    def from_options(cls, options: FlowOptions) -> "Flow":
        return cls(dataset=options.dataset, options=options)
