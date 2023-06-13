from pantomime.types import XLSX

from investigraph.logic import fetch
from investigraph.model import Config, SourceResponse


def test_fetch(ec_meetings: Config):
    for source in ec_meetings.pipeline.sources:
        res = fetch.fetch_source(source)
        assert isinstance(res, SourceResponse)
        assert isinstance(res.response.content, bytes)
        assert res.mimetype == XLSX

        break
