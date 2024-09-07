import logging
from functools import cache
from typing import Any

from anystore.store import get_store
from anystore.util import make_data_checksum

from investigraph.settings import SETTINGS

log = logging.getLogger(__name__)

DELETE = SETTINGS.debug or not SETTINGS.cache_persist


class Cache:
    def __init__(self):
        self.store = get_store()

    def set(self, data: Any, key: str | None = None) -> str:
        key = key or make_data_checksum(data)
        self.store.put(self.get_key(key), data)
        return key

    def get(self, key: str, delete: bool | None = DELETE) -> Any:
        retrieve = self.store.pop if delete else self.store.get
        return retrieve(self.get_key(key))

    @staticmethod
    def get_key(key: str) -> str:
        return f"{SETTINGS.cache_prefix}/{key}"


@cache
def get_cache() -> Cache:
    return Cache()
