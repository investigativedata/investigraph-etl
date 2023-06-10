from pantomime.types import CSV, XLSX

from investigraph.model import Config, DatasetBlock, SourceHead
from investigraph.model.config import get_config
from investigraph.util import get_func


def test_model_config(ec_meetings: Config, local_block: DatasetBlock):
    config = ec_meetings
    assert config.dataset == "ec_meetings"
    assert len(config.pipeline.sources) == 3
    assert config.parse_module_path == "fixtures.ec_meetings.parse:parse"

    config = get_config("ec_meetings", block="local-file-system/testdata")
    assert config.dataset == "ec_meetings"
    assert len(config.pipeline.sources) == 3
    assert (
        config.parse_module_path == "local_file_system_testdata.ec_meetings.parse:parse"
    )

    config = get_config(path="./tests/fixtures/ec_meetings/config.yml")
    assert config.dataset == "ec_meetings"
    assert len(config.pipeline.sources) == 3
    assert config.parse_module_path == "fixtures.ec_meetings.parse:parse"

    func = get_func(config.parse_module_path)
    assert callable(func)


def test_model_gdho_config(gdho: Config):
    config = gdho
    assert config.dataset == "gdho"
    assert len(config.pipeline.sources) == 1
    assert isinstance(config.mappings, list)
    assert len(config.mappings) == 1
    assert config.parse_module_path == "investigraph.logic.transform:map_ftm"

    func = get_func(config.parse_module_path)
    assert callable(func)


def test_model_source(gdho: Config, ec_meetings: Config):
    for source in ec_meetings.pipeline.sources:
        head = source.head()
        assert isinstance(head, SourceHead)
        assert head.uri == source.uri
        assert head.etag is None
        assert head.last_modified is None
        assert head.content_type == XLSX
        break

    for source in gdho.pipeline.sources:
        head = source.head()
        assert head.uri == source.uri
        assert head.etag is None
        assert head.last_modified is None
        assert head.content_type == CSV
        break
