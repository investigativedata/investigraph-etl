from investigraph.block import DatasetBlock
from investigraph.model import FlowOptions
from investigraph.pipeline import run


def test_pipeline_gdho(local_block: DatasetBlock):
    options = FlowOptions(dataset="gdho", block=str(local_block))
    run(options)
