"""
The main entrypoint for the prefect flow
"""

import sys
from typing import Any, Iterable

import shortuuid
from prefect import flow, get_run_logger, task
from prefect_dask.task_runners import DaskTaskRunner

from investigraph import __version__
from investigraph.fetch import fetch_source
from investigraph.load import iter_records
from investigraph.model import Context, SourceResult, get_config, get_parse_func


@task
def parse(ctx: Context, records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    logger = get_run_logger()
    parse_record = get_parse_func(ctx.config.parse_module_path)
    proxies: list[dict[str, Any]] = []
    for rec in records:
        for proxy in parse_record(ctx, rec):
            proxies.append(proxy.to_dict())
    logger.info("PARSED %d records", len(records))


@task
def fetch(ctx: Context) -> SourceResult:
    logger = get_run_logger()
    logger.info("FETCH %s", ctx.source.uri)
    return fetch_source(ctx.source)


@flow(
    name="investigraph-pipeline",
    version=__version__,
    flow_run_name="{ctx.config.dataset}-pipeline-{ctx.source.name}-{ctx.run_id}",
    task_runner=DaskTaskRunner(),
)
def run_pipeline(ctx: Context):
    res = fetch.submit(ctx)
    res = res.result()
    ix = 0
    batch = []
    for ix, rec in enumerate(iter_records(res.mimetype, res.content)):
        batch.append(rec)
        if ix and ix % 1000 == 0:
            parse.submit(ctx, batch)
            batch = []
    if batch:
        parse.submit(ctx, batch)


@flow(
    name="investigraph",
    version=__version__,
    flow_run_name="{dataset}-{run_id}",
)
def run(dataset: str, run_id: str):
    config = get_config(dataset)
    for source in config.pipeline.sources:
        ctx = Context(run_id=run_id, config=config, source=source)
        run_pipeline(ctx)


if __name__ == "__main__":
    dataset = sys.argv[1]
    run(dataset, shortuuid.uuid())
