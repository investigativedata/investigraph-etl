from datetime import datetime
from pathlib import Path

from pantomime.types import XLSX
from requests import Response

from investigraph import fetch
from investigraph.model import SourceResult


def test_fetch(config: Path):
    # ftm.store has cache headers
    url = "https://data.ftm.store/catalog.json"
    last_modified, etag = fetch.head_cache(url)
    assert last_modified is not None
    assert etag is not None
    should_fetch = fetch.should_fetch(url, last_modified, etag)
    assert should_fetch is False

    # actual result
    res = fetch.get(url)
    assert isinstance(res, Response)
    assert res.status_code == 200

    # ec_mettings has no cache headers
    for source in config.pipeline.sources:
        url = source.uri
        last_modified, etag = fetch.head_cache(url)
        assert last_modified is None
        assert etag is None
        should_fetch = fetch.should_fetch(url)
        assert should_fetch is True
        should_fetch = fetch.should_fetch(url, datetime.now(), "etag-foo")
        assert should_fetch is True

        res = fetch.fetch_source(source)
        assert isinstance(res, SourceResult)
        assert isinstance(res.content, bytes)
        assert res.mimetype == XLSX

        break
