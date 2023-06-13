from pantomime.types import XLSX

from investigraph.logic import fetch
from investigraph.model import Config, HttpSourceResponse


def test_fetch(ec_meetings: Config):
    for source in ec_meetings.pipeline.sources:
        res = fetch.fetch_source(source)
        assert isinstance(res, HttpSourceResponse)
        assert res.mimetype == XLSX
        for line in res.iter_lines():
            assert isinstance(line, bytes)
            break

        break
