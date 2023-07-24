"""
The main entrypoint for the prefect flow
"""

from typing import Any

from prefect import flow, get_run_logger, task
from prefect.futures import PrefectFuture
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


def should_continue(res: PrefectFuture, enforce: bool | None = False) -> bool:
    log = get_run_logger()
    state = res.wait()
    should_continue = enforce or state.name != "Cached"
    if not should_continue:
        log.info("Completed because of input has not changed.")
    return should_continue


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
    retries=settings.TASK_RETRIES,
    retry_delay_seconds=settings.TASK_RETRY_DELAY,
    cache_key_fn=get_resolver_cache_key,
    persist_result=True,
    cache_expiration=settings.TASK_CACHE_EXPIRATION,
    refresh_cache=not settings.TASK_CACHE,
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
        res = resolve.submit(ctx)
        if not should_continue(res, ctx.config.extract.enforce):
            return "CACHED"

        res = res.result()
        enumerator = enumerate(ctx.config.extract.handle(ctx, res), 1)
    else:
        enumerator = enumerate(ctx.config.extract.handle(ctx), 1)

    logger = get_run_logger()

    batch = []
    ix = 0
    for ix, rec in enumerator:
        batch.append((rec, ix))
        if ix % ctx.config.transform.chunk_size == 0:
            logger.info("extracting record %d ...", ix)
            res = transform.submit(ctx, ctx.cache.set(batch))
            load.submit(ctx, res)
            batch = []
    if batch:
        res = transform.submit(ctx, ctx.cache.set(batch))
        load.submit(ctx, res)

    logger.info("EXTRACTED %d records", ix)


@flow(
    name="investigraph",
    version=__version__,
    flow_run_name="{options.flow_name}",
)
def run(options: FlowOptions) -> str:
    flow = Flow.from_options(options)
    results = []
    for ix, source in enumerate(flow.config.extract.sources):
        ctx = init_context(config=flow.config, source=source)
        if ix == 0:  # only on first time
            ctx.export_metadata()
            logger = get_run_logger()
            logger.info("INDEX: %s" % ctx.config.load.index_uri)
        results.append(run_pipeline(ctx))

    if all([r == "CACHED" for r in results]):
        return "CACHED"

    if flow.should_aggregate:
        return aggregate(ctx)

    return flow.config.load.entities_uri
