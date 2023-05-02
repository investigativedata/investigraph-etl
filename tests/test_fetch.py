from datetime import datetime
from pathlib import Path

from investigraph import fetch
from investigraph.model import Config


def test_fetch(config_path: Path):
    config = Config.from_path(config_path)

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
        break

    # ftm.store has cache headers
    url = "https://data.ftm.store/catalog.json"
    last_modified, etag = fetch.head_cache(url)
    assert last_modified is not None
    assert etag is not None
    should_fetch = fetch.should_fetch(url, last_modified, etag)
    assert should_fetch is False
