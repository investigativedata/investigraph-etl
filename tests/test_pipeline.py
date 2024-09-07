# from importlib import reload

import cloudpickle
from ftmq.io import smart_read_proxies

from investigraph.model import FlowOptions
from investigraph.model.context import init_context
from investigraph.pipeline import run


def test_pipeline_pickle_ctx(gdho):
    # this is crucial for the whole thing
    tested = False
    for source in gdho.extract.sources:
        ctx = init_context(gdho, source)
        cloudpickle.dumps(ctx)
        tested = True
    assert tested


def test_pipeline_local():
    options = FlowOptions(config="./tests/fixtures/eu_authorities.local.yml")
    out = run(options)
    proxies = [p for p in smart_read_proxies(out.entities_uri)]
    assert len(proxies) == 151


# def test_pipeline_local_ray(monkeypatch):
#     monkeypatch.setenv("PREFECT_TASK_RUNNER", "ray")
#     options = FlowOptions(config="./tests/fixtures/eu_authorities.local.yml")
#     from investigraph.pipeline import run as _run

#     out = _run(options)
#     proxies = [p for p in smart_read_proxies(out)]
#     assert len(proxies) == 151


# def test_pipeline_local_dask(monkeypatch):
#     monkeypatch.setenv("PREFECT_TASK_RUNNER", "dask")
#     options = FlowOptions(config="./tests/fixtures/eu_authorities.local.yml")
#     from investigraph.pipeline import run as _run

#     out = _run(options)
#     proxies = [p for p in smart_read_proxies(out)]
#     assert len(proxies) == 151


def test_pipeline_from_config():
    options = FlowOptions(config="./tests/fixtures/ec_meetings/config.yml")
    out = run(options)
    proxies = [p for p in smart_read_proxies(out.entities_uri)]
    assert len(proxies) > 50_000


def test_pipeline_local_customized():
    options = FlowOptions(config="./tests/fixtures/eu_authorities.custom.yml")
    assert run(options)


def test_pipeline_chunk_size():
    options = FlowOptions(
        config="./tests/fixtures/eu_authorities.local.yml",
        chunk_size=100,
    )
    out = run(options)
    proxies = [p for p in smart_read_proxies(out.entities_uri)]
    assert len(proxies) == 151


# def test_pipeline_caching(monkeypatch):
#     monkeypatch.setenv("TASK_CACHE", "true")
#     reload(settings)
#     assert settings.TASK_CACHE is True
#     options = FlowOptions(config="./tests/fixtures/eu_authorities.local.yml")
#     # FIXME this still doesn't work
#     assert run(options)
#     assert run(options)
