"""
Utils to fetch remote sources, optionally respect cache headers
"""

from datetime import datetime
from typing import Literal

import requests

from investigraph.model import Context, HttpSourceResponse, SmartSourceResponse, Source
from investigraph.util import slugified_dict


def fetch_source(source: Source) -> Literal[HttpSourceResponse, SmartSourceResponse]:
    if source.is_http:
        head = source.head()
        source.stream = head.should_stream()
        res = requests.get(source.uri, stream=source.stream)
        assert res.ok
        return HttpSourceResponse(
            **source.dict(),
            header=slugified_dict(res.headers),
            response=res,
        )
    return SmartSourceResponse(**source.dict())


def get_cache_key(_, params) -> str:
    """
    cache results of fetch tasks based on etag or last_modified
    """
    ctx: Context = params["ctx"]
    if ctx.source.is_http:
        head = ctx.source.head()
        url = ctx.source.uri
        if head.etag:
            return f"{url}-{head.etag}"
        if head.last_modified:
            return f"{url}-{head.last_modified}"
    return datetime.now().isoformat()  # actually don't cache
