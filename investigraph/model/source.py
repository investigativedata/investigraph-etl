import mimetypes
from datetime import datetime

import requests
from dateparser import parse as parse_date
from normality import slugify
from pantomime import normalize_mimetype, types
from pydantic import BaseModel
from runpandarun import Playbook
from runpandarun.util import PathLike, absolute_path
from smart_open import parse_uri

from investigraph.util import slugified_dict


class SourceHead(BaseModel):
    etag: str | None = None
    last_modified: datetime | None = None
    content_type: str | None = None
    content_length: int | None = None

    def __init__(self, **data):
        data = slugified_dict(data)
        last_modified = data.pop("last_modified", None)
        if isinstance(last_modified, str):
            data["last_modified"] = parse_date(last_modified)
        super().__init__(
            content_type=normalize_mimetype(data.pop("content_type", None)),
            **data,
        )

    def can_stream(self) -> bool:
        return self.content_type in (types.CSV, types.JSON)


class Source(BaseModel):
    name: str
    uri: str
    scheme: str
    mimetype: str | None = None
    pandas: Playbook | None = Playbook()
    stream: bool | None = None

    def __init__(self, **data):
        data["uri"] = str(data["uri"])
        data["name"] = data.get("name", slugify(data["uri"]))
        data["scheme"] = data.get("scheme", parse_uri(data["uri"]).scheme)
        if "mimetype" not in data:
            mtype, _ = mimetypes.guess_type(data["uri"])
            data["mimetype"] = normalize_mimetype(mtype)
        data["stream"] = data.get("stream", data["mimetype"] == types.CSV)
        super().__init__(**data)

    def ensure_uri(self, base: PathLike) -> None:
        """
        ensure absolute file paths based on base path of paretn config.yml
        """
        if self.scheme.startswith("file"):
            self.uri = absolute_path(self.uri, base)

    @property
    def is_http(self) -> bool:
        return self.scheme.startswith("http")

    def head(self) -> SourceHead:
        if self.is_http:
            res = requests.head(self.uri)
            return SourceHead(**slugified_dict(res.headers))
        raise NotImplementedError("Cannot fetch head for scheme %s" % self.scheme)
