"""
Utils to fetch remote sources, optionally respect cache headers
"""

from datetime import datetime

import requests
from dateparser import parse as parse_date

from .util import lowercase_dict


def head_cache(url: str) -> tuple[str | None, datetime | None]:
    """
    Get Last-Modified and ETag headers from head request to given url
    """
    res = requests.head(url)
    headers = lowercase_dict(res.headers)
    last_modified: datetime | None = parse_date(headers.get("last-modified", ""))
    etag: str | None = headers.get("etag")
    return last_modified, etag


def should_fetch(
    url: str, last_modified: datetime | None = None, etag: str | None = None
) -> bool:
    """
    Test if given url should be fetched again based on last_modified and etag values
    """
    if last_modified is None and etag is None:
        return True
    res_last_modified, res_etag = head_cache(url)
    if res_etag and etag:
        return res_etag != etag
    if res_last_modified and last_modified:
        return res_last_modified > last_modified
    return True


def get(
    url: str,
    last_modified: datetime | None = None,
    etag: str | None = None,
    force: bool | None = False,
    **kwargs
) -> requests.Response | None:
    """
    Fetch given url if it needs to be re-fetched or forced
    """
    if force or should_fetch(url, last_modified, etag):
        return requests.get(url, **kwargs)
    return
