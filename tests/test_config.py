from investigraph.model import Config, DatasetBlock
from investigraph.model.config import get_config


def test_config(ec_meetings: Config, local_block: DatasetBlock):
    config = ec_meetings
    assert config.dataset.name == "ec_meetings"
    assert config.dataset.prefix == "ec"
    assert len(config.extract.sources) == 3
    assert config.transform.handler.endswith("ec_meetings/transform.py:handle")

    config = get_config("ec_meetings", block="local-file-system/testdata")
    assert config.dataset.name == "ec_meetings"
    assert len(config.extract.sources) == 3
    assert config.transform.handler.endswith("ec_meetings/transform.py:handle")
    func = config.transform.get_handler()
    assert callable(func)

    config = get_config(path="./tests/fixtures/ec_meetings/config.yml")
    assert config.dataset.name == "ec_meetings"
    assert len(config.extract.sources) == 3
    assert config.transform.handler.endswith("ec_meetings/transform.py:handle")

    func = config.transform.get_handler()
    assert callable(func)


def test_config_gdho(gdho: Config):
    config = gdho
    assert config.dataset.name == "gdho"
    assert config.dataset.prefix == "gdho"
    assert len(config.extract.sources) == 1
    assert isinstance(config.transform.mappings, list)
    assert len(config.transform.query) == 1
    assert len(config.transform.mappings) == 1
    assert config.transform.handler == "investigraph.logic.transform:map_ftm"

    func = config.transform.get_handler()
    assert callable(func)


def test_config_pandas_merge():
    config = Config.from_string(
        """
name: test
extract:
  pandas:
    read:
      handler: read_excel
      options:
        skiprows: 1
  sources:
    - uri: uri1
      pandas:
        read:
          options:
            skiprows: 2
    - uri: uri2
      pandas:
        read:
          handler: read_csv
    """
    )
    assert config.extract.pandas.read.handler == "read_excel"
    assert config.extract.pandas.read.options["skiprows"] == 1
    assert config.extract.sources[0].pandas.read.handler == "read_excel"
    assert config.extract.sources[0].pandas.read.options["skiprows"] == 2
    assert config.extract.sources[1].pandas.read.handler == "read_csv"
    assert config.extract.sources[1].pandas.read.options["skiprows"] == 1
