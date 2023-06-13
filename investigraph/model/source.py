from datetime import datetime

import requests
from dateparser import parse as parse_date
from normality import slugify
from pantomime import normalize_mimetype, types
from pydantic import BaseModel
from smart_open import parse_uri

from investigraph.types import SDict
from investigraph.util import slugified_dict


class SourceHead(BaseModel):
    etag: str | None = None
    last_modified: datetime | None = None
    content_type: str | None = None
    content_length: int | None = None

    def __init__(self, **data):
        super().__init__(
            last_modified=parse_date(data.pop("last_modified", "")),
            content_type=normalize_mimetype(data.pop("content_type", None)),
            **data,
        )

    def should_stream(self) -> bool:
        return self.content_type in (types.CSV, types.JSON)


class Source(BaseModel):
    name: str
    uri: str
    scheme: str
    extract_kwargs: dict | None = {}

    def __init__(self, **data):
        data["name"] = data.get("name", slugify(data["uri"]))
        data["scheme"] = data.get("scheme", parse_uri(data["uri"].scheme))
        super().__init__(**data)

    def head(self) -> SourceHead:
        res = requests.head(self.uri)
        return SourceHead(**slugified_dict(res.headers))


class SourceResponse(Source):
    is_stream: bool | None = False
    response: requests.Response
    header: SDict

    class Config:
        arbitrary_types_allowed = True

    @property
    def mimetype(self) -> str:
        return normalize_mimetype(self.header["content_type"])
