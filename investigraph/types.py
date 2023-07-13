from typing import Any, Generator, TypeAlias

from nomenklatura.entity import CE

# a string-keyed dict
SDict: TypeAlias = dict[str, Any]
RecordGenerator: TypeAlias = Generator[SDict, None, None]

# composite entity generator
CEGenerator: TypeAlias = Generator[CE, None, None]

StrGenerator: TypeAlias = Generator[str, None, None]
BytesGenerator: TypeAlias = Generator[bytes, None, None]

TaskResult: TypeAlias = Generator[Any, None, None]
