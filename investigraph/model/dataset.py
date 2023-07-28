from datetime import date, datetime
from typing import Any, Literal

from nomenklatura.dataset.catalog import DataCatalog as NKCatalog
from nomenklatura.dataset.coverage import DataCoverage as NKCoverage
from nomenklatura.dataset.dataset import Dataset as NKDataset
from nomenklatura.dataset.publisher import DataPublisher as NKPublisher
from nomenklatura.dataset.resource import DataResource as NKResource
from normality import slugify
from pydantic import BaseModel

from investigraph.settings import RUN_TIME

Frequencies = Literal[tuple(NKCoverage.FREQUENCIES)]

DEFAULT_CATALOG = NKCatalog(NKDataset, {})


class NKMixin:
    _nk_model = None

    def __init__(self, **data):
        """
        validate input data against nomenklatura implementation
        """
        data = self._nk_model(data)
        super().__init__(**data.to_dict())

    @property
    def nk(self):
        return self._nk_model(self.dict())

    def get(self, attr: str, default: Any | None = None) -> Any:
        return getattr(self, attr, default)


class Publisher(BaseModel, NKMixin):
    _nk_model = NKPublisher

    name: str
    url: str
    description: str | None = None
    country: str | None = None
    country_label: str | None = None
    official: bool = False
    logo_uri: str | None = None


class Resource(BaseModel, NKMixin):
    _nk_model = NKResource

    name: str
    url: str
    title: str | None = None
    checksum: str | None = None
    timestamp: datetime | None = None
    mime_type: str | None = None
    mime_type_label: str | None = None
    size: int | None = 0


class Coverage(BaseModel, NKMixin):
    _nk_model = NKCoverage

    start: date | None = None
    end: date | None = None
    countries: list[str] | None = []
    frequency: Frequencies | None = "unknown"


class Dataset(BaseModel, NKMixin):
    _nk_model = NKDataset

    name: str
    prefix: str | None = None
    title: str | None = None
    license: str | None = None
    summary: str | None = None
    description: str | None = None
    url: str | None = None
    updated_at: datetime | None = None
    version: str | None = None
    category: str | None = None
    publisher: Publisher | None = None
    coverage: Coverage | None = None
    resources: list[Resource] | None = []

    def __init__(self, **data):
        if "title" not in data:
            data["title"] = data["name"].title()
        dataset = NKDataset(DEFAULT_CATALOG, data)
        prefix = data.get("prefix", slugify(dataset.name))
        if data.get("updated_at") is None:
            data["updated_at"] = RUN_TIME
        return super().__init__(prefix=prefix, **dataset.to_dict())

    @property
    def nk(self):
        return self._nk_model(DEFAULT_CATALOG, self.dict())


class Catalog(BaseModel, NKMixin):
    _nk_model = NKCatalog

    datasets: list[Dataset] | None = []

    @property
    def nk(self):
        return self._nk_model(NKDataset, self.dict())
