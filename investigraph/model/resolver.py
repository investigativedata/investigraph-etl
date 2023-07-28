"""
resolver for `.source.Source`
"""

from io import BytesIO

from normality import slugify
from pantomime import types
from pydantic import BaseModel
from smart_open import open

from investigraph.exceptions import ImproperlyConfigured
from investigraph.logic import requests
from investigraph.model.source import Source, SourceHead
from investigraph.types import BytesGenerator
from investigraph.util import checksum

STREAM_TYPES = [types.CSV, types.JSON]


class Resolver(BaseModel):
    source: Source
    head: SourceHead | None = None
    response: requests.Response | None = None
    content: bytes | None = None
    checksum: str | None = None

    class Config:
        arbitrary_types_allowed = True

    @property
    def mimetype(self) -> str:
        if self.source.is_http:
            self._resolve_head()
            return self.head.content_type
        return self.source.mimetype

    @property
    def stream(self) -> bool:
        wants = self.source.stream
        if self.source.is_http:
            self._resolve_head()
            can = self.head.can_stream()
        else:
            can = self.mimetype in STREAM_TYPES
        if wants is None:
            return can
        return wants and can

    def _resolve_head(self) -> None:
        if self.head is None:
            self.head = self.source.head()

    def _resolve_http(self) -> None:
        if self.response is None:
            self._resolve_head()
            self.source.stream = self.source.stream or self.head.can_stream()
            res = requests.get(self.source.uri, stream=self.source.stream)
            assert res.ok
            self.response = res

    def _resolve_content(self) -> None:
        if self.content is None:
            if self.stream:
                raise ImproperlyConfigured("%s is a stream" % self.source.uri)
            if self.source.is_http:
                self._resolve_http()
                self.content = self.response.content
            else:
                with open(self.source.uri, "rb") as fh:
                    self.content = fh.read()
            self.checksum = checksum(BytesIO(self.content))

    def iter(self, chunk_size: int | None = 10_000) -> BytesGenerator:
        if self.source.stream:
            chunk = b""
            for ix, line in enumerate(self.iter_lines(), 1):
                chunk += line + b"\r"
                if ix % chunk_size == 0:
                    yield BytesIO(chunk)
                    chunk = b""
            if chunk:
                yield BytesIO(chunk)
        else:
            yield BytesIO(self.get_content())

    def iter_lines(self) -> BytesGenerator:
        if not self.source.stream:
            raise ImproperlyConfigured("%s is not a stream" % self.source.uri)

        if self.source.is_http:
            self._resolve_http()
            yield from self.response.iter_lines()
        else:
            with open(self.source.uri, "rb") as fh:
                yield from fh

    def get_content(self) -> bytes:
        self._resolve_content()
        return self.content

    def get_cache_key(self) -> str:
        slug = f"RESOLVE#{slugify(self.source.uri)}"
        if self.source.is_http:
            self._resolve_head()
            if self.head.etag:
                return f"{slug}#{self.head.etag}"
            if self.head.last_modified:
                return f"{slug}#{self.head.last_modified.isoformat()}"
        if not self.source.stream:
            self._resolve_content()
            return self.checksum
        return slug  # handle expiration via cache_expiration on @task
