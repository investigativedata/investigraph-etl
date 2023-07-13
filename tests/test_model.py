from pantomime.types import CSV, XLSX

from investigraph.model import Config, SourceHead


def test_model_source(gdho: Config, ec_meetings: Config):
    for source in ec_meetings.pipeline.sources:
        head = source.head()
        assert isinstance(head, SourceHead)
        assert head.etag is None
        assert head.last_modified is None
        assert head.content_type == XLSX
        break

    for source in gdho.pipeline.sources:
        head = source.head()
        assert head.etag is None
        assert head.last_modified is None
        assert head.content_type == CSV
        break
