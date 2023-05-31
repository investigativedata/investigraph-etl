from investigraph.extract import iter_records
from investigraph.fetch import fetch_source
from investigraph.model import Config


def test_extract(config: Config):
    for source in config.pipeline.sources:
        res = fetch_source(source)
        records = [r for r in iter_records(res)]
        assert len(records)
        for rec in records:
            assert isinstance(rec, dict)
            assert "Location" in rec.keys()
            break
