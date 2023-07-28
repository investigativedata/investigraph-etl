"""
Dataset configuration (metadata and optional python function) is stored in
prefect blocks for easy distributed access
"""

from functools import cache, cached_property
from pathlib import Path

from normality import slugify
from prefect import filesystems
from prefect.blocks.core import Block

from investigraph.exceptions import BlockError
from investigraph.settings import DATA_ROOT
from investigraph.util import ensure_path

BLOCK_TYPES = ("github", "local-file-system")


class DatasetBlock:
    def __init__(self, prefix: str, name: str):
        if prefix not in BLOCK_TYPES:
            raise ValueError(f"unsupported block type: `{prefix}`")
        self.prefix = prefix
        self.name = name

    def __str__(self) -> str:
        return f"{self.prefix}/{self.name}"

    @cached_property
    def block(self) -> Block:
        return Block.load(str(self))

    @property
    def is_registered(self) -> bool:
        try:
            self.block
            return self.get_path().exists()
        except ValueError:
            return False

    @property
    def path(self) -> Path:
        if not self.is_registered:
            raise BlockError(f"Block `{self}` not registered yet.")
        return self.get_path()

    def get_path(self) -> Path:
        return DATA_ROOT / "blocks" / slugify(self.block._block_document_id, sep="_")

    def load_dataset(self, dataset: str) -> None:
        raise NotImplementedError  # subclass

    @classmethod
    def from_string(cls, block: str) -> "DatasetBlock":
        prefix, name = block.split("/")
        return cls(prefix=prefix, name=name)

    @staticmethod
    def get_create_kwargs(uri: str) -> dict[str, str]:
        raise NotImplementedError

    def register(
        self,
        uri: str,
        overwrite: bool | None = False,
        ignore_errors: bool | None = True,
    ) -> None:
        kwargs = self.get_create_kwargs(uri)
        block = self._block_cls(**kwargs)
        try:
            block.save(self.name, overwrite=overwrite)
        except ValueError as e:
            if not ignore_errors:
                raise e

    def load(self, dataset: str) -> None:
        """
        Load dataset from block and add path to python path
        """
        self.load_dataset(dataset)


class LocalFileSystemBlock(DatasetBlock):
    _block_cls = filesystems.LocalFileSystem

    @staticmethod
    def get_create_kwargs(uri: str) -> dict[str, str]:
        path = Path(uri).absolute()
        return {"basepath": path}

    def load_dataset(self, dataset: str) -> None:
        remote_path = Path(self.block.basepath) / dataset
        local_path = ensure_path(self.get_path() / dataset)
        self.block.get_directory(remote_path, local_path)


class GitHubBlock(DatasetBlock):
    _block_cls = filesystems.GitHub

    @staticmethod
    def get_create_kwargs(uri: str) -> dict[str, str]:
        return {"repository": uri, "include_git_objects": False}

    def load_dataset(self, dataset: str) -> None:
        self.block.get_directory(dataset, self.get_path())


@cache
def get_block(name: str, uri: str | None = None, **kwargs) -> DatasetBlock:
    if name.startswith("local"):
        block = LocalFileSystemBlock.from_string(name)
    elif name.startswith("github"):
        block = GitHubBlock.from_string(name)
    else:
        raise NotImplementedError(name)

    if uri is not None:
        block.register(uri, **kwargs)
    return block
