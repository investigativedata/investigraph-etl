from pathlib import Path

import pytest

FIXTURES_PATH = Path(__file__).parent.joinpath("fixtures")


@pytest.fixture(scope="module")
def config_path():
    return FIXTURES_PATH.joinpath("config.yml")
