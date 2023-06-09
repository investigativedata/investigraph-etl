from banal import clean_dict
from pydantic import BaseModel

from .config import Config, get_config


class FlowOptions(BaseModel):
    dataset: str
    block: str | None = None
    config: str | None = None
    index_uri: str | None = None
    fragments_uri: str | None = None
    entities_uri: str | None = None
    aggregate: bool | None = True


class Flow(BaseModel):
    dataset: str
    config: Config

    def __init__(self, **data):
        # override base config with runtime options
        options = data.get("options")
        if options is not None:
            options = dict(options)
        config = get_config(
            data["dataset"], options.get("block"), options.get("config")
        )
        data["config"] = {**clean_dict(config.dict()), **options}
        super().__init__(**data)

    @property
    def should_aggregate(self) -> bool:
        if self.config.entities_uri.startswith("postg"):
            return False
        return self.config.aggregate

    @classmethod
    def from_options(cls, options: FlowOptions) -> "Flow":
        return cls(dataset=options.dataset, options=options)
