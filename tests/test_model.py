import pytest
from pydantic import ValidationError

from investigraph.model.dataset import (
    DEFAULT_CATALOG,
    Coverage,
    Dataset,
    NKCoverage,
    NKDataset,
    NKPublisher,
    NKResource,
    Publisher,
    Resource,
)


def test_model_publisher():
    p = Publisher(name="Test", url="https://example.org/")
    assert p.name == NKPublisher(p.dict()).name
    assert p.url == NKPublisher(p.dict()).url
    assert isinstance(p.nk, NKPublisher)


def test_model_resource():
    r = Resource(name="entities.ftm.json", url="https://example.com/entities.ftm.json")
    assert r.name == NKResource(r.dict()).name
    assert r.url == NKResource(r.dict()).url
    assert r.size == NKResource(r.dict()).size == 0
    assert isinstance(r.nk, NKResource)


def test_model_coverage():
    c = Coverage()
    assert c.frequency == "unknown"
    assert c.frequency == NKCoverage(c.dict()).frequency
    assert isinstance(c.nk, NKCoverage)
    c = Coverage(frequency="weekly")
    assert c.frequency == NKCoverage(c.dict()).frequency
    with pytest.raises(ValidationError):
        Coverage(frequency="foo")


def test_model_dataset():
    d = Dataset(name="test-dataset")
    assert isinstance(d.nk, NKDataset)
    assert d.title == "Test-Dataset"
    assert d.prefix == "test-dataset"
    assert d.name == NKDataset(DEFAULT_CATALOG, d.dict()).name
    assert d.title == NKDataset(DEFAULT_CATALOG, d.dict()).title

    d = Dataset(name="test-dataset", prefix="td")
    assert d.prefix == "td"

    data = {
        "name": "test",
        "publisher": {"name": "Test publisher", "url": "https://example.org"},
        "resources": [
            {
                "name": "entities.ftm.json",
                "url": "https://example.org/entities.ftm.json",
            }
        ],
        "coverage": {"frequency": "daily"},
    }
    d = Dataset(**data)
    assert d.title == "Test"
    data = d.nk.to_dict()
    assert data["title"] == "Test"
    assert d.coverage.frequency == "daily"
