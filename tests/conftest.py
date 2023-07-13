from pathlib import Path

import pytest

from investigraph.model import Config
from investigraph.model.block import get_block

FIXTURES_PATH = (Path(__file__).parent / "fixtures").absolute()


@pytest.fixture(scope="module")
def fixtures_path():
    return FIXTURES_PATH


@pytest.fixture(scope="module")
def local_block():
    return get_block("local-file-system/testdata", "./tests/fixtures", overwrite=True)


@pytest.fixture(scope="module")
def ec_meetings():
    return Config.from_path(FIXTURES_PATH / "ec_meetings" / "config.yml")


@pytest.fixture(scope="module")
def gdho():
    config = Config.from_path(FIXTURES_PATH / "gdho" / "config.yml")
    for source in config.extract.sources:
        source.uri = "http://localhost:8000/gdho/organizations.csv"
    return config


@pytest.fixture(scope="module")
def eu_authorities():
    config = Config.from_path(FIXTURES_PATH / "eu_authorities" / "config.yml")
    for source in config.extract.sources:
        source.uri = "http://localhost:8000/all-authorities.csv"
    return config


@pytest.fixture(scope="module")
def ec_meetings_local():
    config = Config.from_path(FIXTURES_PATH / "ec_meetings" / "config.yml")
    for source in config.extract.sources:
        source.uri = "http://localhost:8000/ec-meetings.xlsx"
    return config
