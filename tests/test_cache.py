from investigraph.cache import get_cache
from investigraph.util import make_proxy


def test_cache():
    cache = get_cache()

    key = cache.set("bar")
    assert cache.get(key) == "bar"
    # GETDEL after 1st get
    assert cache.get(key) is None

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
