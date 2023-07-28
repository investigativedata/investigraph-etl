from investigraph.cache import get_cache
from investigraph.util import make_proxy


def test_cache():
    cache = get_cache()

    key = cache.set("bar")
    assert cache.get(key) == "bar"
    assert cache.get(key, delete=True) == "bar"

    # serialization
    key = cache.set(1)
    assert cache.get(key) == 1
    key = cache.set(1.0)
    assert cache.get(key) == 1.0

    data = {"foo": 1, "bar": True}
    key = cache.set(data)
    assert cache.get(key) == data

    proxy = make_proxy("Person")
    proxy.add("name", "Alice")
    proxy.id = "id-alice"
    key = cache.set(proxy.to_dict())
    assert cache.get(key) == proxy.to_dict()

    # manual key & no delete
    cache.set("bar", key="foo")
    assert cache.get("foo") == "bar"
    assert cache.get("foo", delete=True) == "bar"
    assert cache.get("foo") is None

    # sets
    key = cache.sadd("value1")
    cache.sadd("value2", "value3", key=key)
    assert "value1" in cache.smembers(key)
    assert "value2" in cache.smembers(key)
    assert "value3" in cache.smembers(key, delete=True)
    assert cache.smembers(key) is None

    cache.flushall()
