from investigraph.logic.extract import iter_records
from investigraph.logic.fetch import fetch_source
from investigraph.model import Config


def test_extract(ec_meetings: Config, gdho: Config):
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
