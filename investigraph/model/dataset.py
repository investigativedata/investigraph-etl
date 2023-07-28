from datetime import date, datetime
from typing import Any, Generator, Literal, TypeVar

from nomenklatura.dataset.catalog import DataCatalog as NKCatalog
from nomenklatura.dataset.coverage import DataCoverage as NKCoverage
from nomenklatura.dataset.dataset import Dataset as NKDataset
from nomenklatura.dataset.publisher import DataPublisher as NKPublisher
from nomenklatura.dataset.resource import DataResource as NKResource
from normality import slugify
from pydantic import AnyUrl
from pydantic import BaseModel as _BaseModel
from pydantic import HttpUrl

from investigraph.model.mixins import HashableMixin, RemoteMixin, YamlMixin
from investigraph.settings import RUN_TIME

Frequencies = Literal[tuple(NKCoverage.FREQUENCIES)]

C = TypeVar("C", bound="Catalog")
D = TypeVar("D", bound="Dataset")


class BaseModel(RemoteMixin, YamlMixin, HashableMixin, _BaseModel):
    pass


class NKModel(BaseModel):
    class Config:
        json_encoders = {
            NKCatalog: lambda x: x.to_dict(),
            NKDataset: lambda x: x.to_dict(),
            NKPublisher: lambda x: x.to_dict(),
            NKCoverage: lambda x: x.to_dict(),
            NKResource: lambda x: x.to_dict(),
        }

    def to_nk(self) -> NKCatalog | NKCoverage | NKDataset | NKPublisher | NKResource:
        return self._nk_model(self.dict())

    def __getattr__(self, attr: str, default: Any | None = None) -> Any:
        return getattr(self.to_nk(), attr, default)


class Publisher(NKModel):
    _nk_model = NKPublisher

    name: str
    url: HttpUrl
    description: str | None = None
    country: str | None = None
    country_label: str | None = None
    official: bool = False
    logo_uri: AnyUrl | None = None


class Resource(NKModel):
    _nk_model = NKResource

    name: str
    url: AnyUrl
    title: str | None = None
    checksum: str | None = None
    timestamp: datetime | None = None
    mime_type: str | None = None
    mime_type_label: str | None = None
    size: int | None = 0


class Coverage(NKModel):
    _nk_model = NKCoverage

    start: date | None = None
    end: date | None = None
    countries: list[str] | None = []
    frequency: Frequencies | None = "unknown"


class Maintainer(BaseModel):
    """
    this is our own addition
    """

    name: str
    description: str | None = None
    url: HttpUrl | None = None
    logo_uri: HttpUrl | None = None


class Dataset(NKModel):
    _nk_model = NKDataset

    name: str
    prefix: str | None = None
    title: str | None = None
    license: str | None = None
    summary: str | None = None
    description: str | None = None
    url: HttpUrl | None = None
    updated_at: datetime | None = None
    version: str | None = None
    category: str | None = None
    publisher: Publisher | None = None
    coverage: Coverage | None = None
    resources: list[Resource] | None = []

    # own addition
    git_repo: AnyUrl | None = None
    uri: AnyUrl | None = None
    maintainer: Maintainer | None = None
    catalog: C | None = None

    def __init__(self, **data):
        if "include" in data:  # legacy behaviour
            data["uri"] = data.pop("include", None)
        data["title"] = data.get("title") or data.get("name", "").title()
        data["updated_at"] = data.get("updated_at") or RUN_TIME
        data["catalog"] = data.get("catalog") or Catalog().dict()
        super().__init__(**data)
        self.prefix = data.get("prefix") or slugify(self.name)

    def to_nk(self):
        return self._nk_model(self.catalog.to_nk(), self.dict())


class Catalog(NKModel):
    _nk_model = NKCatalog

    datasets: list[Dataset] | None = []
    updated_at: datetime | None = None

    # own additions
    name: str | None = "default"
    maintainer: Maintainer | None = None
    url: HttpUrl | None = None
    uri: AnyUrl | None = None
    logo_uri: AnyUrl | None = None
    catalogs: list[C] | None = []

    def __init__(self, **data):
        if "name" not in data:
            data["name"] = "Catalog"
        super().__init__(**data)

    def to_nk(self):
        return self._nk_model(NKDataset, self.dict())

    def get_datasets(self) -> Generator[Dataset, None, None]:
        yield from self.datasets
        for catalog in self.catalogs:
            yield from catalog.datasets

    def metadata(self) -> dict[str, Any]:
        catalog = self.copy()
        catalog.datasets = []
        catalog.catalogs = [c.metadata() for c in self.catalogs]
        return catalog.dict()


Dataset.update_forward_refs()
Catalog.update_forward_refs()
