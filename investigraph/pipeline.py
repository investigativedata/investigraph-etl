"""
The main entrypoint for the prefect flow
"""

from typing import Any, Literal

from prefect import flow, get_run_logger, task
from prefect.task_runners import ConcurrentTaskRunner

from investigraph import __version__, settings
from investigraph.logic.aggregate import in_memory
from investigraph.logic.extract import iter_records
from investigraph.logic.fetch import fetch_source, get_cache_key
from investigraph.model import (
    Context,
    Flow,
    FlowOptions,
    HttpSourceResponse,
    SmartSourceResponse,
)
from investigraph.model.context import init_context
from investigraph.util import get_func


@task
def aggregate(ctx: Context):
    logger = get_run_logger()
    fragments, proxies = in_memory(ctx, ctx.config.fragments_uri)
    logger.info("AGGREGATED %d fragments to %d proxies", fragments, proxies)
    out = ctx.config.entities_uri
    logger.info("OUTPUT: %s", out)
    return out


@task
def load(ctx: Context, ckey: str):
    logger = get_run_logger()
    proxies = ctx.cache.get(ckey)
    out = ctx.config.fragments_uri
    if ctx.config.target == "postgres":
        # write directly to entities instead of fragments
        # as aggregation is happening within postgres store on write
        out = ctx.config.entities_uri
        ctx.entities_loader.write(proxies)
    else:
        ctx.fragments_loader.write(proxies, mode="ab")
    logger.info("LOADED %d proxies", len(proxies))
    logger.info("OUTPUT: %s", out)
    return out


@task
def transform(ctx: Context, ckey: str) -> str:
    logger = get_run_logger()
    parse_record = get_func(ctx.config.parse_module_path)
    proxies: list[dict[str, Any]] = []
    records = ctx.cache.get(ckey)
    for rec in records:
        for proxy in parse_record(ctx, rec):
            proxies.append(proxy.to_dict())
    logger.info("TRANSFORMED %d records", len(records))
    return ctx.cache.set(proxies)


@task(
    retries=settings.FETCH_RETRIES,
    retry_delay_seconds=settings.FETCH_RETRY_DELAY,
    cache_key_fn=get_cache_key,
)
def fetch(ctx: Context) -> Literal[HttpSourceResponse, SmartSourceResponse]:
    logger = get_run_logger()
    logger.info("OPEN %s", ctx.source.uri)
    return fetch_source(ctx.source)


@flow(
    name="investigraph-pipeline",
    version=__version__,
    flow_run_name="{ctx.dataset}-{ctx.source.name}",
    task_runner=ConcurrentTaskRunner(),
)
def run_pipeline(ctx: Context):
    res = fetch.submit(ctx)
    res = res.result()
    ix = 0
    batch = []
    results = []
    for ix, rec in enumerate(iter_records(res)):
        batch.append(rec)
        if ix and ix % 1000 == 0:
            results.append(transform.submit(ctx, ctx.cache.set(batch)))
            batch = []
    if batch:
        results.append(transform.submit(ctx, ctx.cache.set(batch)))

    logger = get_run_logger()
    logger.info("EXTRACTED %d records", ix + 1)

    # write
    for res in results:
        res = res.result()
        load.submit(ctx, res)


@flow(
    name="investigraph",
    version=__version__,
    flow_run_name="{options.dataset}",
)
def run(options: FlowOptions):
    flow = Flow.from_options(options)
    for source in flow.config.pipeline.sources:
        ctx = init_context(config=flow.config, source=source)
        ctx.export_metadata()
        run_pipeline(ctx)

    if flow.should_aggregate:
        aggregate(ctx)
