from ftmstore import get_dataset

from investigraph.model import DatasetBlock, FlowOptions
from investigraph.pipeline import run
from investigraph.settings import DATA_ROOT


def test_pipeline_from_block(local_block: DatasetBlock):
    options = FlowOptions(dataset="gdho", block=str(local_block))
    run(options)


def test_pipeline_from_config():
    options = FlowOptions(
        dataset="eu_authorities", config="./tests/fixtures/eu_authorities/config.yml"
    )
    run(options)


def test_pipeline_ftmstore_target():
    store_uri = f"sqlite:///{DATA_ROOT}/store.db"
    options = FlowOptions(
        dataset="eu_authorities",
        config="./tests/fixtures/eu_authorities/config.yml",
        entities_uri=store_uri,
    )
    run(options)
    store = get_dataset("eu_authorities", database_uri=store_uri)
    entities = [e for e in store.iterate()]
    assert 200 > len(entities) > 100  # FIXME
