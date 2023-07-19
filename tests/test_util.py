from investigraph.model import Config
from investigraph.util import dict_merge, pydantic_merge


def test_util():
    d1 = {"a": 1, "b": 2}
    d2 = {"c": 3}
    assert dict_merge(d1, d2) == {"a": 1, "b": 2, "c": 3}

    d1 = {"a": 1, "b": 2}
    d2 = {"a": 3}
    assert dict_merge(d1, d2) == {"a": 3, "b": 2}

    d1 = {"a": {"b": 1}}
    d2 = {"a": {"c": "e"}}
    assert dict_merge(d1, d2) == {"a": {"b": 1, "c": "e"}}

    d1 = {"a": {"b": 1, "c": 2}, "f": "foo", "g": False}
    d2 = {"a": {"b": 2}, "e": 4, "f": None}
    assert dict_merge(d1, d2) == {"a": {"b": 2, "c": 2}, "e": 4, "f": "foo", "g": False}

    d1 = {
        "read": {"options": {"skiprows": 1}, "uri": "-", "handler": "read_excel"},
        "operations": [],
        "write": {"options": {}, "uri": "-", "handler": None},
    }
    d2 = {
        "read": {"options": {"skiprows": 2}, "uri": "-", "handler": None},
        "operations": [],
        "write": {"options": {}, "uri": "-", "handler": None},
    }
    assert dict_merge(d1, d2) == {
        "read": {"options": {"skiprows": 2}, "uri": "-", "handler": "read_excel"},
        "operations": [],
        "write": {"options": {}, "uri": "-"},
    }

    c1 = Config(name="test")
    c2 = Config(name="test", base_path="/tmp/")
    c = pydantic_merge(c1, c2)
    assert str(c.base_path) == "/tmp"
