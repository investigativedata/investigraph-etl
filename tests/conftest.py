from pathlib import Path

import pytest

from investigraph.model import Config

FIXTURES_PATH = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def config_path():
    return FIXTURES_PATH / "config.yml"


@pytest.fixture(scope="module")
def config(config_path: Path):
    return Config.from_path(config_path)
