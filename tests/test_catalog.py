from investigraph import Catalog, Dataset
from investigraph.model.dataset import NKCatalog, NKDataset


def test_catalog_model(fixtures_path):
    # investigraph vs. nomenklatura

    catalog = Catalog()
    assert isinstance(catalog.to_nk(), NKCatalog)

    catalog = Catalog(datasets=[Dataset(name="test")])
    assert isinstance(catalog.to_nk(), NKCatalog)
    assert isinstance(catalog.datasets[0], Dataset)
    assert isinstance(catalog.datasets[0].to_nk(), NKDataset)

    catalog = Catalog.from_path(fixtures_path / "catalog.yml")
    assert catalog.name == "Catalog"
    assert len(catalog.datasets) == 7
    assert catalog.datasets[0].name == "eu_transparency_register"

    catalog = Catalog.from_path(fixtures_path / "catalog_full.yml")
    assert catalog.name == "Test Catalog"
    assert catalog.maintainer.name == "investigraph"
    assert catalog.maintainer.url == "https://investigraph.dev"
    assert len(catalog.datasets) == 1
    assert catalog.datasets[0].name == "eutr"
    assert len(list(catalog.get_datasets())) == 17

    o_catalog = catalog.catalogs[1]
    assert o_catalog.url == "https://opensanctions.org"
    assert o_catalog.maintainer.name == "OpenSanctions"
    assert o_catalog.maintainer.description == "OpenSanctions is cool"

    ftg_catalog = catalog.catalogs[2]
    assert ftg_catalog.datasets[0].name == "pubmed"
    assert ftg_catalog.datasets[0].prefix == "pubmed"


def test_catalog_legacy(fixtures_path):
    catalog = Catalog.from_path(fixtures_path / "catalog_legacy.yml")
    assert catalog.name == "Catalog"
    assert len(catalog.datasets) == 7
    assert catalog.datasets[0].name == "eu_transparency_register"
