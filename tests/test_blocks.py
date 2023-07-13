from __future__ import annotations

import shutil

import pytest

from investigraph.exceptions import BlockError
from investigraph.model.block import GitHubBlock  # , get_block
from investigraph.model.block import LocalFileSystemBlock
from investigraph.settings import DATA_ROOT, DATASETS_BLOCK, DATASETS_REPO


def test_blocks_github():
    DATASET = "ec_meetings"

    # remove old testdata
    shutil.rmtree(DATA_ROOT / "blocks", ignore_errors=True)

    # block = get_block(DATASETS_BLOCK)  # FIXME caching breaks testing here
    block = GitHubBlock.from_string(DATASETS_BLOCK)
    assert isinstance(block, GitHubBlock)

    with pytest.raises(BlockError) as e:
        block.path
    assert "not registered" in str(e)

    block.register(DATASETS_REPO, overwrite=True)
    block.load(DATASET)
    path = block.path
    assert path.exists()
    assert path.is_dir()
    assert "ec_meetings" in [i for p in path.glob("*") for i in p.parts]

    shutil.rmtree(path.parent)


def test_blocks_local():
    DATASET = "gdho"
    DATASETS_BLOCK = "local-file-system/testdata"

    # remove old testdata
    shutil.rmtree(DATA_ROOT / "blocks", ignore_errors=True)

    # block = get_block(DATASETS_BLOCK)  # FIXME caching breaks testing here
    block = LocalFileSystemBlock.from_string(DATASETS_BLOCK)
    assert isinstance(block, LocalFileSystemBlock)
    block.register("./tests/fixtures", overwrite=True)
    block.load(DATASET)
    path = block.path
    assert path.exists()
    assert path.is_dir()
    assert "gdho" in [i for p in path.glob("*") for i in p.parts]
    shutil.rmtree(path.parent)
