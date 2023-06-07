import shutil
from pathlib import Path

from investigraph.prefect import DatasetBlock
from investigraph.settings import DATASETS_BLOCK, DATASETS_DIR, DATASETS_REPO

DATASET = "ec_meetings"


def test_block_github():
    # remove old testdata
    path = Path.cwd() / DATASETS_DIR / DATASET
    shutil.rmtree(path.parent, ignore_errors=True)

    # without dataset
    block = DatasetBlock.from_string(DATASETS_BLOCK, DATASET)
    block.register(DATASETS_REPO, overwrite=True)
    block.ensure()
    assert path.exists()
    assert path.is_dir()
    shutil.rmtree(path.parent)

    # with dataset
    block = DatasetBlock.from_string(DATASETS_BLOCK, DATASET)
    block.ensure(DATASET)
    assert path.exists()
    assert path.is_dir()
    shutil.rmtree(path.parent)
