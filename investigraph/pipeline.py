"""
The main entrypoint for the prefect flow
"""

from typing import Any

from prefect import flow, get_run_logger, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect_dask import DaskTaskRunner
from prefect_ray import RayTaskRunner

from investigraph import __version__, settings
from investigraph.logic.aggregate import in_memory
from investigraph.model import Context, Flow, FlowOptions, Resolver
from investigraph.model.context import init_context
from investigraph.model.resolver import get_resolver_cache_key


def get_runner_from_env() -> ConcurrentTaskRunner | DaskTaskRunner | RayTaskRunner:
    if settings.TASK_RUNNER == "dask":
        return DaskTaskRunner()
    if settings.TASK_RUNNER == "ray":
        return RayTaskRunner()
    return ConcurrentTaskRunner()


@task
def aggregate(ctx: Context):
    logger = get_run_logger()
    fragments, proxies = in_memory(ctx, ctx.config.load.fragments_uri)
    logger.info("AGGREGATED %d fragments to %d proxies", fragments, proxies)
    out = ctx.config.load.entities_uri
    logger.info("OUTPUT: %s", out)
    return out


@task
def load(ctx: Context, ckey: str):
    logger = get_run_logger()
    proxies = ctx.cache.get(ckey)
    out = ctx.load_fragments(proxies)
    logger.info("LOADED %d proxies", len(proxies))
    logger.info("OUTPUT: %s", out)
    return out


@task
def transform(ctx: Context, ckey: str) -> str:
    logger = get_run_logger()
    proxies: list[dict[str, Any]] = []
    records = ctx.cache.get(ckey)
    for rec, ix in records:
        for proxy in ctx.config.transform.handle(ctx, rec, ix):
            proxy.datasets = {ctx.dataset}
            proxies.append(proxy.to_dict())
    logger.info("TRANSFORMED %d records", len(records))
    return ctx.cache.set(proxies)


@task(
    retries=settings.FETCH_RETRIES,
    retry_delay_seconds=settings.FETCH_RETRY_DELAY,
    cache_key_fn=get_resolver_cache_key,
)
def resolve(ctx: Context) -> Resolver:
    logger = get_run_logger()
    logger.info("RESOLVE %s", ctx.source.uri)
    return Resolver(source=ctx.source)


@flow(
    name="investigraph-pipeline",
    version=__version__,
    flow_run_name="{ctx.dataset}-{ctx.source.name}",
    task_runner=get_runner_from_env(),
)
def run_pipeline(ctx: Context):
    # extract
    if ctx.config.extract.fetch:
        res = resolve(ctx)
        enumerator = enumerate(ctx.config.extract.handle(ctx, res), 1)
    else:
        enumerator = enumerate(ctx.config.extract.handle(ctx), 1)

    # transform
    batch = []
    results = []
    ix = 0
    for ix, rec in enumerator:
        batch.append((rec, ix))
        if ix % ctx.config.transform.chunk_size == 0:
            results.append(transform.submit(ctx, ctx.cache.set(batch)))
            batch = []
    if batch:
        results.append(transform.submit(ctx, ctx.cache.set(batch)))

    logger = get_run_logger()
    logger.info("EXTRACTED %d records", ix)

    # load
    for res in results:
        res = res.result()
        load.submit(ctx, res)


@flow(
    name="investigraph",
    version=__version__,
    flow_run_name="{options.dataset}",
)
def run(options: FlowOptions) -> str:
    flow = Flow.from_options(options)
    for ix, source in enumerate(flow.config.extract.sources):
        ctx = init_context(config=flow.config, source=source)
        if ix == 0:  # only on first time
            ctx.export_metadata()
            logger = get_run_logger()
            logger.info("INDEX: %s" % ctx.config.load.index_uri)
        run_pipeline(ctx)

    if flow.should_aggregate:
        aggregate(ctx)

    return flow.config.load.entities_uri
