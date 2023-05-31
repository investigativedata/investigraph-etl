"""
Load transformed data (aka CE proxies) to various targets like json files,
databases, s3 endpoints...
"""

from typing import Any, Iterable

from investigraph.util import smart_write_proxies


def to_fragments(uri: str, proxies: Iterable[dict[str, Any]]):
    smart_write_proxies(uri, proxies, "ab")
