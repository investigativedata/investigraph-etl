"""
Load transformed data (aka CE proxies) to various targets like json files,
databases, s3 endpoints...
"""

from typing import Any, Iterable

import orjson
from smart_open import open

from investigraph import settings
from investigraph.model import Context
from investigraph.util import ensure_path


def to_fragments(
    ctx: Context, proxies: Iterable[dict[str, Any]], uri: str | None = None
):
    if uri is None:
        path = ensure_path(settings.DATA_ROOT / ctx.dataset / ctx.run_id)
        uri = (path / "fragments.json").as_uri()

    out = b""
    for proxy in proxies:
        out += orjson.dumps(proxy, option=orjson.OPT_APPEND_NEWLINE)
    with open(uri, "ab") as f:
        f.write(out)
