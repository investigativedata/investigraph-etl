from pantomime.types import CSV, XLSX

from investigraph.model import Config, SourceHead, get_config  # , get_parse_func


def test_model_config(ec_meetings: Config):
    config = ec_meetings
    assert config.dataset == "ec_meetings"
    assert len(config.pipeline.sources) == 3
    assert config.parse_module_path == "datasets.ec_meetings.parse:parse"

    # config = get_config("ec_meetings")
    # assert config.dataset == "ec_meetings"
    # assert len(config.pipeline.sources) == 3
    # assert config.parse_module_path == "datasets.ec_meetings.parse:parse"

    # func = get_parse_func(config.parse_module_path)
    # assert callable(func)


def test_model_gdho_config(gdho: Config):
    config = gdho
    assert config.dataset == "gdho"
    assert len(config.pipeline.sources) == 1
    assert isinstance(config.mappings, list)
    assert len(config.mappings) == 1
    assert config.parse_module_path == "investigraph.transform:map_ftm"

    # func = get_parse_func(config.parse_module_path)
    # assert callable(func)


def test_model_source(gdho: Config, ec_meetings: Config):
    for source in ec_meetings.pipeline.sources:
        head = SourceHead.from_source(source)
        assert head.source == source
        assert head.etag is None
        assert head.last_modified is None
        assert head.content_type == XLSX
        break

    for source in gdho.pipeline.sources:
        head = SourceHead.from_source(source)
        assert head.source == source
        assert head.etag is None
        assert head.last_modified is None
        assert head.content_type == CSV
        break
