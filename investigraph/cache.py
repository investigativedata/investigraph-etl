from functools import cache
from typing import Any

import fakeredis
import redis
import shortuuid
from cachelib.serializers import RedisSerializer

from investigraph import settings


class Cache:
    serializer = RedisSerializer()

    def __init__(self):
        if settings.DEBUG:
            con = fakeredis.FakeStrictRedis()
        else:
            con = redis.from_url(settings.REDIS_URL)
        con.ping()
        self.cache = con

    def set(self, key: str | None, data: Any) -> str:
        if key is None:
            key = shortuuid.uuid()
        data = self.serializer.dumps(data)
        self.cache.set(self.get_key(key), data)
        return key

    def get(self, key: str) -> Any:
        key = self.get_key(key)
        res = self.cache.get(key)
        if res is not None:
            self.cache.delete(key)  # GETDEL
            data = self.serializer.loads(res)
            return data

    @staticmethod
    def get_key(key: str) -> str:
        return f"{settings.CACHE_PREFIX}:{key}"


@cache
def get_cache() -> Cache:
    return Cache()
