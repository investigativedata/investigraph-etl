from typing import Any

import pytest

from investigraph.logic.extract import extract_pandas
from investigraph.model import Config, Resolver, Source


def _get_records(source: Source) -> list[dict[str, Any]]:
    res = Resolver(source=source)
    return [r for r in extract_pandas(res)]


def test_extract_http_tabular():
    base_uri = "http://localhost:8000/%s"

    source = Source(uri=base_uri % "all-authorities.csv")
    assert source.is_http
    assert source.stream

    head = source.head()
    assert head.can_stream()
    records = _get_records(source)
    assert len(records) == 151
    for rec in records:
        assert isinstance(rec, dict)
        assert "Name" in rec.keys()
        break

    source = Source(uri=base_uri % "ec-meetings.xlsx")
    source.pandas.read.options = {"skiprows": 1}
    assert source.is_http
    assert not source.stream

    head = source.head()
    assert not head.can_stream()
    records = _get_records(source)
    assert len(records) == 12482
    for rec in records:
        assert isinstance(rec, dict)
        assert "Location" in rec.keys()
        break


def test_extract_local(fixtures_path):
    source = Source(uri=fixtures_path / "all-authorities.csv")
    assert not source.is_http

    with pytest.raises(NotImplementedError):
        source.head()

    records = _get_records(source)
    assert len(records) == 151
    for rec in records:
        assert isinstance(rec, dict)
        assert "Name" in rec.keys()
        break

    source = Source(uri=fixtures_path / "ec-meetings.xlsx")
    source.pandas.read.options = {"skiprows": 1}
    assert not source.is_http

    with pytest.raises(NotImplementedError):
        source.head()

    records = _get_records(source)
    assert len(records) == 12482
    for rec in records:
        assert isinstance(rec, dict)
        assert "Location" in rec.keys()
        break


def test_extract_from_config(ec_meetings_local: Config, gdho: Config):
    tested = False
    for source in ec_meetings_local.extract.sources:
        records = _get_records(source)
        assert len(records)
        for rec in records:
            assert isinstance(rec, dict)
            assert "Location" in rec.keys()
            tested = True
            break
    assert tested

    tested = False
    for source in gdho.extract.sources:
        records = _get_records(source)
        assert len(records)
        for rec in records:
            assert isinstance(rec, dict)
            assert "Name" in rec.keys()
            tested = True
            break
    assert tested
