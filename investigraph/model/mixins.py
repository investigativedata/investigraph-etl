import logging
from functools import cache
from pathlib import Path
from typing import Any

import orjson
import yaml
from smart_open import open

from investigraph.types import PathLike
from investigraph.util import data_checksum, to_dict

log = logging.getLogger(__name__)


@cache
def cached_from_uri(uri: str) -> dict[str, Any]:
    """
    Cache RemoteMixin on runtime
    """
    log.info("Loading `%s` ..." % uri)
    with open(uri, "rb") as fh:
        data = fh.read()
    return orjson.loads(data)


class RemoteMixin:
    """
    Load a pydantic model from a remote uri, such as dataset index.json or catalog.json
    """

    def __init__(self, **data):
        """
        Update with remote data, but local data takes precedence
        """
        if data.get("uri"):
            remote = self.from_uri(data["uri"])
            data = {**remote.dict(), **data}
        super().__init__(**data)

    @classmethod
    def from_uri(cls, uri: str) -> "RemoteMixin":
        data = cached_from_uri(uri)
        return cls(**data)


class YamlMixin:
    """
    Load a pydantic model from yaml spec
    """

    @classmethod
    def from_string(cls, data: str, **kwargs) -> "YamlMixin":
        data = yaml.safe_load(data)
        return cls(**data, **kwargs)

    @classmethod
    def from_path(cls, fp: PathLike) -> "YamlMixin":
        with open(fp) as fh:
            data = fh.read()
        return cls.from_string(data, base_path=Path(fp).parent)


class HashableMixin:
    """
    Make a (pydantic) model hashable
    """

    def __hash__(self) -> int:
        return hash(data_checksum(to_dict(self)))
