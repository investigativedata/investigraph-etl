from pathlib import Path

import pytest

from investigraph.model import Config
from investigraph.prefect import DatasetBlock
from investigraph.settings import DATASETS_BLOCK

FIXTURES_PATH = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def config_path():
    return FIXTURES_PATH / "config.yml"


@pytest.fixture(scope="module")
def config(config_path: Path):
    return Config.from_path(config_path)


@pytest.fixture(scope="module")
def default_block():
    block = DatasetBlock.from_string(DATASETS_BLOCK, "ec_meetings")
    block.ensure()
    return block
