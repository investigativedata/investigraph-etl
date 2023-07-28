import logging
from functools import cache
from typing import Any, Iterable, Set

import fakeredis
import redis
from cachelib.serializers import RedisSerializer

from investigraph import settings
from investigraph.util import data_checksum

log = logging.getLogger(__name__)


class Cache:
    """
    This is an extremely simple cache interface for sharing tasks data
    efficiently via redis (or fakeredis during development)

    it creates (prefixed) keys based on input data

    it mimics redis GETDEL so that after fetching data from cache the key is
    deleted (turn of by `delete=False`)
    """

    serializer = RedisSerializer()

    def __init__(self):
        if settings.DEBUG:
            con = fakeredis.FakeStrictRedis()
            con.ping()
            log.info("Redis connected: `fakeredis`")
        else:
            con = redis.from_url(settings.REDIS_URL)
            con.ping()
            log.info("Redis connected: `{settings.REDIS_URL}`")
        self.cache = con

    def set(self, data: Any, key: str | None = None) -> str:
        key = key or data_checksum(data)
        data = self.serializer.dumps(data)
        self.cache.set(self.get_key(key), data)
        return key

    def get(self, key: str, delete: bool | None = False) -> Any:
        key = self.get_key(key)
        res = self.cache.get(key)
        if delete:
            self.cache.delete(key)  # GETDEL
        if res is not None:
            data = self.serializer.loads(res)
            return data

    def sadd(self, *values: Iterable[Any], key: str | None = None) -> str:
        values = [str(v) for v in values]
        key = key or data_checksum(values)
        self.cache.sadd(self.get_key(key) + "#SET", *values)
        return key

    def smembers(self, key: str, delete: bool | None = False) -> Set[str]:
        key = self.get_key(key) + "#SET"
        res: Set[bytes] = self.cache.smembers(key)
        if delete:
            self.cache.delete(key)
        return {v.decode() for v in res} or None

    def flushall(self):
        self.cache.flushall()

    @staticmethod
    def get_key(key: str) -> str:
        return f"{settings.REDIS_PREFIX}:{key}"


@cache
def get_cache() -> Cache:
    return Cache()
