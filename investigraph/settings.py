import os
from pathlib import Path

from anystore.model import StoreModel
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

VERSION = "0.9.0"

DATA_ROOT = Path(
    os.environ.get("INVESTIGRAPH_DATA_ROOT", Path.cwd() / "data")
).absolute()


class Settings(BaseSettings):
    """
    `investigraph` settings management using
    [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)

    Note:
        All settings can be set via environment variables (or a .env file),
        prepending `INVESTIGRAPH_` (except for those with a different alias)
    """

    model_config = SettingsConfigDict(
        env_prefix="investigraph_",
        env_nested_delimiter="_",
        nested_model_default_partial_update=True,
        env_file=".env",
        extra="ignore",
    )

    debug: bool = Field(default=False, alias="debug")
    """Enable debug mode (more error output)"""

    data_root: Path = DATA_ROOT
    """Default data directory to store archive and json exports"""

    config: str | None = None
    """Use this config.yml globally"""

    seeder: str = "investigraph.logic.seed:handle"
    """Use this seed handler globally"""

    extractor: str = "investigraph.logic.extract:handle"
    """Use this extract handler globally"""

    transformer: str = "investigraph.logic.transform:map_ftm"
    """Use this transform handler globally"""

    loader: str = "investigraph.logic.load:handle"
    """Use this load handler globally"""

    exporter: str = "investigraph.logic.export:handle"
    """Use this export handler globally"""

    cache: StoreModel = StoreModel(uri="memory:///")
    """Runtime cache"""

    store_uri: str = Field(default="memory:///", alias="ftm_statement_store")
    """Statement store for entity aggregation"""

    incremental: bool = False
    """If true, skip already processed sources"""

    tags_uri: str | None = None
    """Tags storage for incremental source extraction (default in-memory or
    lakehouse if available)"""

    lakehouse_uri: str | None = Field(default=None, alias="lakehouse_uri")

    @property
    def is_lakehouse(self) -> bool:
        return self.lakehouse_uri is not None
