from datetime import timedelta
from pathlib import Path
from typing import Literal

from prefect.settings import PREFECT_HOME
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

VERSION = "0.6.1"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="investigraph_", extra="allow")

    debug: bool = Field(False, alias="debug")
    data_root: Path = Field((Path.cwd() / "data").absolute())
    default_seeder: str = "investigraph.logic.seed:handle"
    default_extractor: str = "investigraph.logic.extract:handle"
    default_transformer: str = "investigraph.logic.transform:map_ftm"
    default_loader: str = "investigraph.logic.load:handle"
    default_aggregator: str = "investigraph.logic.aggregate:handle"

    cache_uri: str = "memory:///"
    cache_prefix: str = f"investigraph/{VERSION}"
    cache_persist: bool = False
    cache_ttl: int | None = None

    task_cache: bool = False
    task_retries: int = 3
    task_retry_delay: int = 5
    task_cache_expiration: timedelta | None = None

    fetch_cache: bool = True
    extract_cache: bool = True
    transform_cache: bool = True
    load_cache: bool = True
    aggregate_cache: bool = True

    task_runner: Literal["dask", "ray"] | None = Field(
        None, alias="prefect_task_runner"
    )

    chunk_size: int = 1_000

    anystore_uri: str = Field(
        (Path(PREFECT_HOME.value()) / ".anystore").absolute().as_uri(),
        alias="anystore_uri",
    )

    archive_uri: str = Field(str((Path.cwd() / "data" / "archive").absolute().as_uri()))

    log_json: bool = Field(alias="log_json", default=False)
    log_level: str = Field(alias="log_level", default="info")


SETTINGS = Settings()
DEBUG = SETTINGS.debug
