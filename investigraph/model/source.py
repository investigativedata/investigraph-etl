import mimetypes
from datetime import datetime

import requests
from dateparser import parse as parse_date
from normality import slugify
from pantomime import normalize_mimetype, types
from pydantic import BaseModel
from smart_open import open, parse_uri

from investigraph.exceptions import ImproperlyConfigured
from investigraph.types import BytesGenerator, SDict
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
    mimetype: str | None = None
    extract_kwargs: dict | None = {}
    stream: bool | None = False
    ijson_path: str | None = "item"

    def __init__(self, **data):
        data["uri"] = str(data["uri"])
        data["name"] = data.get("name", slugify(data["uri"]))
        data["scheme"] = data.get("scheme", parse_uri(data["uri"]).scheme)
        if "mimetype" not in data:
            mtype, _ = mimetypes.guess_type(data["uri"])
            data["mimetype"] = normalize_mimetype(mtype)
        data["stream"] = data.get("stream", data["mimetype"] == types.CSV)
        super().__init__(**data)

    @property
    def is_http(self) -> bool:
        return self.scheme.startswith("http")

    @property
    def is_local(self) -> bool:
        return self.scheme.startswith("file")

    def head(self) -> SourceHead:
        if self.is_http:
            res = requests.head(self.uri)
            return SourceHead(**slugified_dict(res.headers))
        raise NotImplementedError("Cannot fetch head for scheme %s" % self.scheme)

    def iter_lines(self) -> BytesGenerator:
        raise NotImplementedError

    def open(self):
        return open(self.uri)


class HttpSourceResponse(Source):
    response: requests.Response
    header: SDict

    class Config:
        arbitrary_types_allowed = True

    @property
    def mimetype(self) -> str:
        return normalize_mimetype(self.header["content_type"])

    @property
    def content(self) -> bytes:
        if self.stream:
            raise ImproperlyConfigured("%s is a stream" % self.uri)
        return self.response.content

    def iter_lines(self) -> BytesGenerator:
        yield from self.response.iter_lines()


class SmartSourceResponse(Source):
    @property
    def content(self) -> bytes:
        if self.stream:
            raise ImproperlyConfigured("%s is a stream" % self.uri)
        with open(self.uri, "rb") as fh:
            return fh.read()

    def iter_lines(self) -> BytesGenerator:
        with open(self.uri, "rb") as fh:
            yield from fh
