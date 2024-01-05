import pytest

from investigraph import util
from investigraph.exceptions import DataError
from investigraph.model import Config


def test_util():
    d1 = {"a": 1, "b": 2}
    d2 = {"c": 3}
    assert util.dict_merge(d1, d2) == {"a": 1, "b": 2, "c": 3}

    d1 = {"a": 1, "b": 2}
    d2 = {"a": 3}
    assert util.dict_merge(d1, d2) == {"a": 3, "b": 2}

    d1 = {"a": {"b": 1}}
    d2 = {"a": {"c": "e"}}
    assert util.dict_merge(d1, d2) == {"a": {"b": 1, "c": "e"}}

    d1 = {"a": {"b": 1, "c": 2}, "f": "foo", "g": False}
    d2 = {"a": {"b": 2}, "e": 4, "f": None}
    assert util.dict_merge(d1, d2) == {
        "a": {"b": 2, "c": 2},
        "e": 4,
        "f": "foo",
        "g": False,
    }

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
    assert util.dict_merge(d1, d2) == {
        "read": {"options": {"skiprows": 2}, "uri": "-", "handler": "read_excel"},
        "operations": [],
        "write": {"options": {}, "uri": "-"},
    }

    c1 = Config(name="test")
    c2 = Config(name="test", base_path="/tmp/")
    c = util.pydantic_merge(c1, c2)
    assert str(c.base_path) == "/tmp"


def test_util_join():
    assert util.join_text("A", " ", "b", "-") == "A b"


def test_util_data_checksum():
    assert len(util.data_checksum("a")) == 32
    assert len(util.data_checksum({"foo": "bar"})) == 32
    assert len(util.data_checksum(True)) == 32
    assert util.data_checksum(["a", 1]) != util.data_checksum(["a", "1"])


def test_util_str_or_none():
    assert util.str_or_none("foo") == "foo"
    assert util.str_or_none("") is None
    assert util.str_or_none(" ") is None
    assert util.str_or_none(None) is None


def test_util_make_proxy():
    proxy = util.make_proxy("LegalEntity")
    assert proxy.id is None
    with pytest.raises(DataError):
        util.make_proxy("LegalEntity", name="test")
    proxy = util.make_proxy("Person", id="i", name="Jane", country="France")
    assert proxy.id == "i"
    assert "Jane" in proxy.get("name")
    assert proxy.caption == "Jane"
    assert proxy.first("country") == "fr"
