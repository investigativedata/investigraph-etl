from investigraph.logic.extract import iter_records
from investigraph.logic.fetch import fetch_source
from investigraph.model import Config, Source

BASE_URL = "https://s3.investigativedata.org/investigraph/testdata/%s"


def test_extract_tabular():
    source = Source(uri=BASE_URL % "all-authorities.csv")
    head = source.head()
    assert head.should_stream()
    res = fetch_source(source)
    records = [r for r in iter_records(res)]
    assert len(records) == 151
    for rec in records:
        assert isinstance(rec, dict)
        assert "Name" in rec.keys()
        break

    source = Source(uri=BASE_URL % "ec-meetings.xlsx")
    source.extract_kwargs = {"skiprows": 1}
    head = source.head()
    assert not head.should_stream()
    res = fetch_source(source)
    records = [r for r in iter_records(res)]
    assert len(records) == 12482
    for rec in records:
        assert isinstance(rec, dict)
        assert "Location" in rec.keys()
        break


def test_extract_from_config(ec_meetings: Config, gdho: Config):
    for source in ec_meetings.pipeline.sources:
        res = fetch_source(source)
        records = [r for r in iter_records(res)]
        assert len(records)
        for rec in records:
            assert isinstance(rec, dict)
            assert "Location" in rec.keys()
            break

    for source in gdho.pipeline.sources:
        res = fetch_source(source)
        records = [r for r in iter_records(res)]
        assert len(records)
        for rec in records:
            assert isinstance(rec, dict)
            assert "Name" in rec.keys()
            break
