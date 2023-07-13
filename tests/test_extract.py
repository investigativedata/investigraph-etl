import pytest
from moto import mock_s3

from investigraph.logic.extract import iter_records
from investigraph.logic.fetch import fetch_source
from investigraph.model import Config, Source
from tests.util import setup_s3_bucket


def test_extract_http_tabular():
    base_uri = "http://localhost:8000/%s"

    source = Source(uri=base_uri % "all-authorities.csv")
    assert source.is_http
    assert source.stream

    head = source.head()
    assert head.should_stream()
    res = fetch_source(source)
    records = [r for r in iter_records(res)]
    assert len(records) == 151
    for rec in records:
        assert isinstance(rec, dict)
        assert "Name" in rec.keys()
        break

    source = Source(uri=base_uri % "ec-meetings.xlsx")
    source.extract_kwargs = {"skiprows": 1}
    assert source.is_http
    assert not source.stream

    head = source.head()
    assert not head.should_stream()
    res = fetch_source(source)
    records = [r for r in iter_records(res)]
    assert len(records) == 12482
    for rec in records:
        assert isinstance(rec, dict)
        assert "Location" in rec.keys()
        break


@mock_s3
def test_extract_smart_tabular(fixtures_path):
    setup_s3_bucket(with_content=True)

    base_uri = "s3://investigraph/%s"

    source = Source(uri=base_uri % "all-authorities.csv")
    assert not source.is_http

    with pytest.raises(NotImplementedError):
        source.head()

    res = fetch_source(source)
    records = [r for r in iter_records(res)]
    assert len(records) == 151
    for rec in records:
        assert isinstance(rec, dict)
        assert "Name" in rec.keys()
        break

    source = Source(uri=base_uri % "ec-meetings.xlsx")
    source.extract_kwargs = {"skiprows": 1}
    assert not source.is_http

    with pytest.raises(NotImplementedError):
        source.head()

    res = fetch_source(source)
    records = [r for r in iter_records(res)]
    assert len(records) == 12482
    for rec in records:
        assert isinstance(rec, dict)
        assert "Location" in rec.keys()
        break

    # from local file
    source = Source(uri=fixtures_path / "all-authorities.csv")
    assert not source.is_http
    assert source.is_local

    with pytest.raises(NotImplementedError):
        source.head()

    res = fetch_source(source)
    records = [r for r in iter_records(res)]
    assert len(records) == 151
    for rec in records:
        assert isinstance(rec, dict)
        assert "Name" in rec.keys()
        break

    source = Source(uri=fixtures_path / "ec-meetings.xlsx")
    source.extract_kwargs = {"skiprows": 1}
    assert not source.is_http
    assert source.is_local

    with pytest.raises(NotImplementedError):
        source.head()

    res = fetch_source(source)
    records = [r for r in iter_records(res)]
    assert len(records) == 12482
    for rec in records:
        assert isinstance(rec, dict)
        assert "Location" in rec.keys()
        break


def test_extract_from_config(ec_meetings_local: Config, gdho: Config):
    for source in ec_meetings_local.extract.sources:
        res = fetch_source(source)
        records = [r for r in iter_records(res)]
        assert len(records)
        for rec in records:
            assert isinstance(rec, dict)
            assert "Location" in rec.keys()
            break

    for source in gdho.extract.sources:
        res = fetch_source(source)
        records = [r for r in iter_records(res)]
        assert len(records)
        for rec in records:
            assert isinstance(rec, dict)
            assert "Name" in rec.keys()
            break
