from banal import as_bool
from pydantic import BaseModel

from investigraph.settings import CHUNK_SIZE

from .config import Config, get_config


class FlowOptions(BaseModel):
    dataset: str
    block: str | None = None
    config: str | None = None
    aggregate: bool | None = None
    chunk_size: int | None = CHUNK_SIZE

    index_uri: str | None = None
    fragments_uri: str | None = None
    entities_uri: str | None = None


class Flow(BaseModel):
    dataset: str
    config: Config

    def __init__(self, **data):
        # override base config with runtime options
        options = data.pop("options", None)
        if options is not None:
            options = dict(options)
        config = get_config(
            data["dataset"], options.get("block"), options.get("config")
        )
        if "chunk_size" in options:
            chunk_size = options["chunk_size"]
            if chunk_size is not None:
                config.extract.chunk_size = chunk_size
                config.transform.chunk_size = chunk_size
                config.load.chunk_size = chunk_size
        if "index_uri" in options:
            config.load.index_uri = options["index_uri"]
        if "entities_uri" in options:
            config.load.entities_uri = options["entities_uri"]
        if "fragments_uri" in options:
            config.load.fragments_uri = options["fragments_uri"]

        config.load.aggregate = (
            as_bool(options.get("aggregate")) or config.load.aggregate
        )

        super().__init__(config=config, **data)

    @property
    def should_aggregate(self) -> bool:
        if self.config.load.entities_uri.startswith("postg"):
            return False
        return self.config.load.aggregate

    @classmethod
    def from_options(cls, options: FlowOptions) -> "Flow":
        return cls(dataset=options.dataset, options=options)
