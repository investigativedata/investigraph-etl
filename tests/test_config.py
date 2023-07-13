from investigraph.model import Config, DatasetBlock
from investigraph.model.config import get_config


def test_config(ec_meetings: Config, local_block: DatasetBlock):
    config = ec_meetings
    assert config.dataset == "ec_meetings"
    assert len(config.extract.sources) == 3
    assert config.transform.handler.endswith("ec_meetings/transform.py")

    config = get_config("ec_meetings", block="local-file-system/testdata")
    assert config.dataset == "ec_meetings"
    assert len(config.extract.sources) == 3
    assert config.transform.handler.endswith("ec_meetings/transform.py")
    func = config.transform.get_handler()
    assert callable(func)

    config = get_config(path="./tests/fixtures/ec_meetings/config.yml")
    assert config.dataset == "ec_meetings"
    assert len(config.extract.sources) == 3
    assert config.transform.handler.endswith("ec_meetings/transform.py")

    func = config.transform.get_handler()
    assert callable(func)


def test_config_gdho(gdho: Config):
    config = gdho
    assert config.dataset == "gdho"
    assert len(config.extract.sources) == 1
    assert isinstance(config.transform.mappings, list)
    assert len(config.transform.query) == 1
    assert len(config.transform.mappings) == 1
    assert config.transform.handler == "investigraph.logic.transform:map_ftm"

    func = config.transform.get_handler()
    assert callable(func)
