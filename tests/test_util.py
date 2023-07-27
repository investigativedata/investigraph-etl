from investigraph import util
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


def test_util_cleaning():
    assert util.clean_string(" foo\n bar") == "foo bar"
    assert util.clean_string(None) is None
    assert util.clean_string("") is None
    assert util.clean_string("  ") is None
    assert util.clean_name("  foo\n bar") == "foo bar"
    assert util.clean_name("- - . *") is None

    assert util.fingerprint("Mrs. Jane Doe") == "doe jane mrs"
    assert util.fingerprint("Mrs. Jane Mrs. Doe") == "doe jane mrs"
    assert util.fingerprint("#") is None
    assert util.fingerprint(" ") is None
    assert util.fingerprint("") is None
    assert util.fingerprint(None) is None

    assert util.join_text("A", " ", "b", "-") == "A b"


def test_util_data_checksum():
    assert len(util.data_checksum("a")) == 32
    assert util.data_checksum(["a", 1]) != util.data_checksum(["a", "1"])
    assert util.data_checksum([1, 2]) == util.data_checksum([2, 1])
    assert util.data_checksum({"a": 1, "b": 2}) == util.data_checksum({"b": 2, "a": 1})
