import shutil

from investigraph.model.block import GitHubBlock, LocalFileSystemBlock, get_block
from investigraph.settings import DATA_ROOT, DATASETS_BLOCK, DATASETS_REPO


def test_blocks_github():
    DATASET = "ec_meetings"

    # remove old testdata
    shutil.rmtree(DATA_ROOT / "blocks", ignore_errors=True)

    block = get_block(DATASETS_BLOCK)
    path = block.path
    assert isinstance(block, GitHubBlock)
    block.register(DATASETS_REPO, overwrite=True)
    block.load(DATASET)
    assert path.exists()
    assert path.is_dir()
    assert "ec_meetings" in [i for p in path.glob("*") for i in p.parts]
    shutil.rmtree(path.parent)


def test_blocks_local():
    DATASET = "gdho"
    DATASETS_BLOCK = "local-file-system/testdata"

    # remove old testdata
    shutil.rmtree(DATA_ROOT / "blocks", ignore_errors=True)

    block = get_block(DATASETS_BLOCK)
    path = block.path
    assert isinstance(block, LocalFileSystemBlock)
    block.register("./tests/fixtures", overwrite=True)
    block.load(DATASET)
    assert path.exists()
    assert path.is_dir()
    assert "gdho" in [i for p in path.glob("*") for i in p.parts]
    shutil.rmtree(path.parent)
