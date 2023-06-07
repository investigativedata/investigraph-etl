from pathlib import Path

import pytest

from investigraph.model import Config

FIXTURES_PATH = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def ec_meetings():
    return Config.from_path(FIXTURES_PATH / "ec_meetings" / "config.yml")


@pytest.fixture(scope="module")
def gdho():
    return Config.from_path(FIXTURES_PATH / "gdho" / "config.yml")
