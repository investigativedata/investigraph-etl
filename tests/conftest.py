from pathlib import Path

import pytest

from investigraph.block import get_block
from investigraph.model import Config

FIXTURES_PATH = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def local_block():
    return get_block("local-file-system/testdata", "./tests/fixtures", overwrite=True)


@pytest.fixture(scope="module")
def ec_meetings():
    return Config.from_path(FIXTURES_PATH / "ec_meetings" / "config.yml")


@pytest.fixture(scope="module")
def gdho():
    return Config.from_path(FIXTURES_PATH / "gdho" / "config.yml")
