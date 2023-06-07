import shutil
from pathlib import Path

from investigraph.block import GitHubBlock, LocalFileSystemBlock, get_block
from investigraph.settings import DATASETS_BLOCK, DATASETS_DIR, DATASETS_REPO


def test_blocks_github():
    DATASET = "ec_meetings"

    # remove old testdata
    path = Path.cwd() / DATASETS_DIR / DATASET
    shutil.rmtree(path.parent, ignore_errors=True)

    block = get_block(DATASETS_BLOCK)
    assert isinstance(block, GitHubBlock)
    block.register(DATASETS_REPO, overwrite=True)
    block.load(DATASET)
    assert path.exists()
    assert path.is_dir()
    assert "ec_meetings" in str(path)
    shutil.rmtree(path.parent)


def test_blocks_local():
    DATASET = "gdho"
    DATASETS_BLOCK = "local-file-system/testdata"

    # remove old testdata
    path = Path.cwd() / DATASETS_DIR / DATASET
    shutil.rmtree(path.parent, ignore_errors=True)

    block = get_block(DATASETS_BLOCK)
    assert isinstance(block, LocalFileSystemBlock)
    block.register("./tests/fixtures", overwrite=True)
    block.load(DATASET)
    assert path.exists()
    assert path.is_dir()
    assert "gdho" in str(path)
    shutil.rmtree(path.parent)

    block.register("./tests/fixtures", ignore_errors=True)
