from typing import Any, Generator, TypeAlias

from ftmq.types import PathLike
from nomenklatura.entity import CE

# a string-keyed dict
Record: TypeAlias = dict[str, Any]
SDict: TypeAlias = Record  # FIXME backwards compatibility
RecordGenerator: TypeAlias = Generator[Record, None, None]

# composite entity generator
CEGenerator: TypeAlias = Generator[CE, None, None]

StrGenerator: TypeAlias = Generator[str, None, None]
BytesGenerator: TypeAlias = Generator[bytes, None, None]

TaskResult: TypeAlias = Generator[Any, None, None]
PathLike: TypeAlias = PathLike
