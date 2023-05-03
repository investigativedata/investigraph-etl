"""
The main entrypoint for the prefect flow
"""

import sys
from typing import Any, Iterable

import orjson
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run
from prefect.task_runners import ConcurrentTaskRunner

from investigraph import __version__, settings
from investigraph.fetch import fetch_source
from investigraph.load import iter_records
from investigraph.model import Context, SourceResult, get_config, get_parse_func


@task
def load(ctx: Context, proxies: Iterable[dict[str, Any]]):
    logger = get_run_logger()
    name = flow_run.get_name()
    run_id = flow_run.get_id()

    out = b""
    for proxy in proxies:
        out += orjson.dumps(proxy, option=orjson.OPT_APPEND_NEWLINE)
    with open(settings.DATA_ROOT / f"{name}-{run_id}.json", "ab") as f:
        f.write(out)
    logger.info("LOADED %d proxies", len(proxies))


@task
def transform(ctx: Context, records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    logger = get_run_logger()
    parse_record = get_parse_func(ctx.config.parse_module_path)
    proxies: list[dict[str, Any]] = []
    for rec in records:
        for proxy in parse_record(ctx, rec):
            proxies.append(proxy.to_dict())
    logger.info("TRANSFORMED %d records", len(records))
    return proxies


@task
def fetch(ctx: Context) -> SourceResult:
    logger = get_run_logger()
    logger.info("FETCH %s", ctx.source.uri)
    return fetch_source(ctx.source)


@flow(
    name="investigraph-pipeline",
    version=__version__,
    flow_run_name="{ctx.config.dataset}-pipeline-{ctx.source.name}",
    task_runner=ConcurrentTaskRunner(),
)
def run_pipeline(ctx: Context):
    res = fetch.submit(ctx)
    res = res.result()
    ix = 0
    batch = []
    results = []
    for ix, rec in enumerate(iter_records(res.mimetype, res.content)):
        batch.append(rec)
        if ix and ix % 1000 == 0:
            results.append(transform.submit(ctx, batch))
            batch = []
    if batch:
        results.append(transform.submit(ctx, batch))

    logger = get_run_logger()
    logger.info("EXTRACTED %d records", ix + 1)

    # write
    for res in results:
        res = res.result()
        load.submit(ctx, res)


@flow(
    name="investigraph",
    version=__version__,
    flow_run_name="{dataset}",
)
def run(dataset: str):
    config = get_config(dataset)
    for source in config.pipeline.sources:
        ctx = Context(config=config, source=source)
        run_pipeline(ctx)


if __name__ == "__main__":
    dataset = sys.argv[1]
    run(dataset)
