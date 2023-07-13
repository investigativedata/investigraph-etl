from functools import cache
from typing import Any, Iterable, Set

import fakeredis
import redis
import shortuuid
from cachelib.serializers import RedisSerializer

from investigraph import settings
from investigraph.logging import get_logger

log = get_logger(__name__)


class Cache:
    """
    This is an extremely simple cache interface for sharing tasks data
    efficiently via redis (or fakeredis during development)

    it creates (prefixed) random keys during data set to cache.

    it mimics redis GETDEL so that after fetching data from cache the key is
    deleted (turn of by `delete=False`)
    """

    serializer = RedisSerializer()

    def __init__(self):
        if settings.DEBUG:
            con = fakeredis.FakeStrictRedis()
        else:
            con = redis.from_url(settings.REDIS_URL)
        con.ping()
        log.info("Redis initialized", url=settings.REDIS_URL)
        self.cache = con

    def set(self, data: Any, key: str | None = None) -> str:
        data = self.serializer.dumps(data)
        key = key or shortuuid.uuid()
        self.cache.set(self.get_key(key), data)
        return key

    def get(self, key: str, delete: bool | None = True) -> Any:
        key = self.get_key(key)
        res = self.cache.get(key)
        if delete:
            self.cache.delete(key)  # GETDEL
        if res is not None:
            data = self.serializer.loads(res)
            return data

    def sadd(self, *values: Iterable[Any], key: str | None = None) -> str:
        key = key or shortuuid.uuid()
        values = [str(v) for v in values]
        self.cache.sadd(self.get_key(key) + "#SET", *values)
        return key

    def smembers(self, key: str, delete: bool | None = True) -> Set[str]:
        key = self.get_key(key) + "#SET"
        res: Set[bytes] = self.cache.smembers(key)
        if delete:
            self.cache.delete(key)
        return {v.decode() for v in res} or None

    def flushall(self):
        self.cache.flushall()

    @staticmethod
    def get_key(key: str) -> str:
        return f"{settings.CACHE_PREFIX}:{key}"


@cache
def get_cache() -> Cache:
    return Cache()
