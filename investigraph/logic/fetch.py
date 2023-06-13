"""
Utils to fetch remote sources, optionally respect cache headers
"""

from datetime import datetime

import requests

from investigraph.model import Context, Source, SourceResponse
from investigraph.util import slugified_dict

HTTP = ("http", "https")


def fetch_source(source: Source) -> SourceResponse:
    if source.scheme in HTTP:
        head = source.head()
        stream = head.should_stream()
        res = requests.get(source.uri, stream=stream)
        assert res.ok
        return SourceResponse(
            **source.dict(),
            header=slugified_dict(res.headers),
            response=res,
            is_stream=stream,
        )
    else:
        raise NotImplementedError("Scheme: %s" % source.scheme)


def get_cache_key(_, params) -> str:
    """
    cache results of fetch tasks based on etag or last_modified
    """
    ctx: Context = params["ctx"]
    head = ctx.source.head()
    url = ctx.source.uri
    if head.etag:
        return f"{url}-{head.etag}"
    if head.last_modified:
        return f"{url}-{head.last_modified}"
    return f"{url}-{datetime.now()}"  # actually don't cache
