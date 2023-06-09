from investigraph.model import DatasetBlock, FlowOptions
from investigraph.pipeline import run


def test_pipeline_from_block(local_block: DatasetBlock):
    options = FlowOptions(dataset="gdho", block=str(local_block))
    run(options)


def test_pipeline_from_config():
    options = FlowOptions(
        dataset="eu_authorities", config="./tests/fixtures/eu_authorities/config.yml"
    )
    run(options)
