from pathlib import Path

from pantomime.types import XLSX

from investigraph.model import Config, SourceHead, get_config, get_parse_func


def test_model_config(config_path: Path):
    config = Config.from_path(config_path)
    assert config.dataset == "ec_meetings"
    assert len(config.pipeline.sources) == 3
    assert config.parse_module_path == "datasets.ec_meetings.parse"

    config = get_config("ec_meetings")
    assert config.dataset == "ec_meetings"
    assert len(config.pipeline.sources) == 3
    assert config.parse_module_path == "datasets.ec_meetings.parse"

    func = get_parse_func(config.parse_module_path)
    assert callable(func)


def test_model_source(config: Config):
    for source in config.pipeline.sources:
        head = SourceHead.from_source(source)
        assert head.source == source
        assert head.etag is None
        assert head.last_modified is None
        assert head.content_type == XLSX
        break
