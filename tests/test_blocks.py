import shutil
from pathlib import Path

from investigraph.prefect import DatasetBlock
from investigraph.settings import DATASETS_BLOCK, DATASETS_DIR, DATASETS_REPO

DATASET = "ec_meetings"


def test_block_github():
    block = DatasetBlock.from_string(DATASETS_BLOCK, DATASET)
    block.register(DATASETS_REPO)
    block.ensure()
    path = Path.cwd() / DATASETS_DIR / DATASET
    assert path.exists()
    assert path.is_dir()
    shutil.rmtree(path.parent)
