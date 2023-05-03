from investigraph.cache import get_cache
from investigraph.util import make_proxy


def test_cache():
    cache = get_cache()

    cache.set("foo", "bar")
    assert cache.get("foo") == "bar"
    # GETDEL after 1st get
    assert cache.get("foo") is None

    cache.set("foo", "bar")
    cache.set("foo", "bar2")
    assert cache.get("foo") == "bar2"

    # serialization
    cache.set("foo", 1)
    assert cache.get("foo") == 1
    cache.set("foo", 1.0)
    assert cache.get("foo") == 1.0

    cache.set(1, 2)
    assert cache.get(1) == 2

    data = {"foo": 1, "bar": True}
    cache.set("data", data)
    assert cache.get("data") == data

    proxy = make_proxy("Person")
    proxy.add("name", "Alice")
    proxy.id = "id-alice"
    cache.set("p", proxy.to_dict())
    assert cache.get("p") == proxy.to_dict()

    # implicit key
    key = cache.set(None, "data")
    assert isinstance(key, str)
    assert cache.get(key) == "data"
