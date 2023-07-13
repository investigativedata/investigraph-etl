# import os

from ftmq.io import smart_read_proxies
from ftmstore import get_dataset
from moto import mock_s3

from investigraph.model import DatasetBlock, FlowOptions
from investigraph.pipeline import run
from investigraph.settings import DATA_ROOT
from tests.util import setup_s3_bucket


def test_pipeline_local():
    options = FlowOptions(
        dataset="eu_authorities", config="./tests/fixtures/eu_authorities.local.yml"
    )
    out = run(options)
    proxies = [p for p in smart_read_proxies(out)]
    assert len(proxies) == 151


# def test_pipeline_local_ray():
#     os.environ["PREFECT_TASK_RUNNER"] = "ray"
#     options = FlowOptions(
#         dataset="eu_authorities", config="./tests/fixtures/eu_authorities.local.yml"
#     )
#     from investigraph.pipeline import run as _run

#     out = _run(options)
#     proxies = [p for p in smart_read_proxies(out)]
#     assert len(proxies) == 151

#     del os.environ["TASK_RUNNER"]


# def test_pipeline_local_dask():
#     os.environ["PREFECT_TASK_RUNNER"] = "dask"
#     options = FlowOptions(
#         dataset="eu_authorities", config="./tests/fixtures/eu_authorities.local.yml"
#     )
#     from investigraph.pipeline import run as _run

#     out = _run(options)
#     proxies = [p for p in smart_read_proxies(out)]
#     assert len(proxies) == 151

#     del os.environ["TASK_RUNNER"]


def test_pipeline_from_block(local_block: DatasetBlock):
    options = FlowOptions(dataset="gdho", block=str(local_block))
    run(options)


def test_pipeline_from_config():
    options = FlowOptions(
        dataset="ec_meetings", config="./tests/fixtures/ec_meetings/config.yml"
    )
    run(options)


def test_pipeline_local_ftmstore():
    store_uri = f"sqlite:///{DATA_ROOT}/store.db"
    options = FlowOptions(
        dataset="eu_authorities",
        config="./tests/fixtures/eu_authorities.local.yml",
        entities_uri=store_uri,
    )
    run(options)
    store = get_dataset("eu_authorities", database_uri=store_uri)
    entities = [e for e in store.iterate()]
    assert len(entities) == 151


@mock_s3
def test_pipeline_local_s3():
    setup_s3_bucket()
    options = FlowOptions(
        dataset="eu_authorities",
        config="./tests/fixtures/eu_authorities.local.yml",
        fragments_uri="s3://investigraph/eu_authorities/fragments.ftm.json",
        entities_uri="s3://investigraph/eu_authorities/entities.ftm.json",
    )
    out = run(options)
    proxies = [p for p in smart_read_proxies(out)]
    assert len(proxies) == 151
