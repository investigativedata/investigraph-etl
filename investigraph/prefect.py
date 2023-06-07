"""
Helpers to interact with prefect.io SDK
"""

from prefect import filesystems
from prefect.blocks.core import Block
from pydantic import BaseModel

from investigraph.exceptions import ImproperlyConfigured
from investigraph.settings import DATASETS_DIR
from investigraph.util import ensure_path


class DatasetBlock(BaseModel):
    dataset: str | None = None
    prefix: str
    name: str

    def __str__(self) -> str:
        return f"{self.prefix}/{self.name}"

    def ensure(self) -> None:
        block = Block.load(str(self))
        if "get-directory" in block._block_schema_capabilities:
            block.get_directory(self.dataset, ensure_path(DATASETS_DIR))
            return
        raise NotImplementedError(str(self))

    def register(self, repository: str) -> None:
        # currently github only
        block = filesystems.GitHub(repository=repository, include_git_objects=False)
        block.save(self.name)

    @staticmethod
    def from_string(block: str, dataset: str | None = None) -> "DatasetBlock":
        if not block.startswith("github"):
            raise NotImplementedError(block)
        try:
            prefix, name = block.split("/")
            return DatasetBlock(prefix=prefix, name=name, dataset=dataset)
        except ValueError:
            raise ImproperlyConfigured(block)
