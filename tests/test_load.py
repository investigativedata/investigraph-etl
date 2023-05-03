from investigraph.fetch import fetch_source
from investigraph.load import iter_records
from investigraph.model import Config


def test_load(config: Config):
    for source in config.pipeline.sources:
        res = fetch_source(source)
        records = [r for r in iter_records(res.mimetype, res.content)]
        assert len(records)
        for rec in records:
            assert isinstance(rec, dict)
            break
