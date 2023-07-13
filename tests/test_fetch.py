from pantomime.types import CSV, XLSX

from investigraph.logic import fetch
from investigraph.model import Config, HttpSourceResponse


def test_fetch(eu_authorities: Config, ec_meetings_local: Config):
    for source in ec_meetings_local.extract.sources:
        head = source.head()
        assert not head.should_stream()
        res = fetch.fetch_source(source)
        assert isinstance(res, HttpSourceResponse)
        assert res.mimetype == XLSX
        for line in res.iter_lines():
            assert isinstance(line, bytes)
            break

        break

    for source in eu_authorities.extract.sources:
        head = source.head()
        assert head.should_stream()
        res = fetch.fetch_source(source)
        assert isinstance(res, HttpSourceResponse)
        assert res.mimetype == CSV
        for line in res.iter_lines():
            assert isinstance(line, bytes)
            break

        break
