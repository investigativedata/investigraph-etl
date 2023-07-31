from datetime import datetime
from typing import Any

from normality import slugify
from prefect.runtime import flow_run
from pydantic import BaseModel, root_validator

from investigraph.model.config import Config, get_config
from investigraph.settings import CHUNK_SIZE, DATA_ROOT, DATASETS_BLOCK
from investigraph.util import ensure_path


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
    run_id: str | None = None
    start: datetime
    end: datetime | None = None
    fragment_uris: set[str] | None = set()
    entities_uri: str | None = None

    def __init__(self, **data):
        data["start"] = data.get("start", datetime.utcnow())
        data["run_id"] = data.get("run_id", flow_run.get_id())

        # override base config with runtime options
        options = data.pop("options", None)
        if options:
            config = get_config(
                data.pop("dataset", options.dataset),
                options.block,
                options.config,
            )

            self.assign(config.extract, "chunk_size", options.chunk_size)
            self.assign(config.transform, "chunk_size", options.chunk_size)
            self.assign(config.load, "chunk_size", options.chunk_size)

            self.assign(config.load, "index_uri", options.index_uri)
            self.assign(config.load, "fragments_uri", options.fragments_uri)
            self.assign(config.load, "entities_uri", options.entities_uri)
            if options.aggregate is False:
                config.aggregate = False
            if config.aggregate:
                self.assign(config.aggregate, "chunk_size", options.chunk_size)

            super().__init__(dataset=config.dataset.name, config=config, **data)
        else:
            super().__init__(**data)
        self.entities_uri = self.config.load.entities_uri
        path = ensure_path(DATA_ROOT / self.config.dataset.name)
        if self.config.load.index_uri is None:
            self.config.load.index_uri = (path / "index.json").as_uri()
        if self.config.load.fragments_uri is None:
            self.config.load.fragments_uri = (path / "fragments.json").as_uri()
        if self.config.load.entities_uri is None:
            self.config.load.entities_uri = (path / "entities.ftm.json").as_uri()

    @classmethod
    def from_options(cls, options: FlowOptions) -> "Flow":
        return cls(options=options)

    @staticmethod
    def assign(base: Any, attr: str, value: Any) -> None:
        if value is not None:
            setattr(base, attr, value)
