from datetime import datetime

from pantomime.types import CSV, XLSX

from investigraph.model import Config, SourceHead


def test_model_source(eu_authorities: Config, ec_meetings_local: Config):
    for source in ec_meetings_local.extract.sources:
        head = source.head()
        assert isinstance(head, SourceHead)
        assert head.etag is None
        assert isinstance(head.last_modified, datetime)
        assert head.content_type == XLSX
        break

    for source in eu_authorities.extract.sources:
        assert source.stream
        head = source.head()
        assert head.etag is None
        assert isinstance(head.last_modified, datetime)
        assert head.content_type == CSV
        break
