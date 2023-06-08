"""
Dataset configuration (metadata and optional python function) is stored in
prefect blocks for easy distributed access
"""

from functools import cache
from pathlib import Path

from prefect import filesystems
from prefect.blocks.core import Block
from pydantic import BaseModel, validator

from investigraph.settings import DATASETS_DIR
from investigraph.util import ensure_path, ensure_pythonpath

BLOCK_TYPES = ("github", "local-file-system")


class DatasetBlock(BaseModel):
    prefix: str
    name: str

    def __str__(self) -> str:
        return f"{self.prefix}/{self.name}"

    @validator("prefix")
    def ensure_allowed_prefix(cls, v):
        if str(v) not in BLOCK_TYPES:
            raise ValueError(f"unsupported block type: `{v}`")
        return str(v)

    @property
    def block(self) -> Block:
        return Block.load(str(self))

    def load_dataset(self, dataset: str) -> None:
        raise NotImplementedError  # subclass

    @classmethod
    def from_string(cls, block: str, dataset: str | None = None) -> "DatasetBlock":
        prefix, name = block.split("/")
        return cls(prefix=prefix, name=name, dataset=dataset)

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
        ensure_pythonpath(DATASETS_DIR.parent)


class LocalFileSystemBlock(DatasetBlock):
    _block_cls = filesystems.LocalFileSystem

    @staticmethod
    def get_create_kwargs(uri: str) -> dict[str, str]:
        return {"basepath": uri}

    def load_dataset(self, dataset: str) -> None:
        remote_path = Path(self.block.basepath) / dataset
        local_path = ensure_path(DATASETS_DIR / dataset)
        self.block.get_directory(remote_path, local_path)


class GitHubBlock(DatasetBlock):
    _block_cls = filesystems.GitHub

    @staticmethod
    def get_create_kwargs(uri: str) -> dict[str, str]:
        return {"repository": uri, "include_git_objects": False}

    def load_dataset(self, dataset: str) -> None:
        self.block.get_directory(dataset, ensure_path(DATASETS_DIR))


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
