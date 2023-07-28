"""
The main entrypoint for the prefect flow
"""

from typing import Any, Generator

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect_dask import DaskTaskRunner
from prefect_ray import RayTaskRunner

from investigraph import __version__, settings
from investigraph.logic.aggregate import in_memory
from investigraph.model import Context, Flow, FlowOptions, Resolver
from investigraph.model.context import init_context
from investigraph.util import data_checksum


def get_runner_from_env() -> ConcurrentTaskRunner | DaskTaskRunner | RayTaskRunner:
    if settings.TASK_RUNNER == "dask":
        return DaskTaskRunner()
    if settings.TASK_RUNNER == "ray":
        return RayTaskRunner()
    return ConcurrentTaskRunner()


def get_task_cache_key(_, params) -> str:
    return params["ckey"]


@task(
    retries=settings.TASK_RETRIES,
    retry_delay_seconds=settings.TASK_RETRY_DELAY,
    cache_key_fn=get_task_cache_key,
    cache_expiration=settings.TASK_CACHE_EXPIRATION,
    refresh_cache=not settings.TASK_CACHE,
)
def aggregate(ctx: Context, results: list[str], ckey: str):
    fragments, proxies = in_memory(ctx, *results)
    ctx.log.info("AGGREGATED %d fragments to %d proxies", fragments, proxies)
    out = ctx.config.load.entities_uri
    ctx.log.info("OUTPUT: %s", out)
    return out


@task(
    retries=settings.TASK_RETRIES,
    retry_delay_seconds=settings.TASK_RETRY_DELAY,
    cache_key_fn=get_task_cache_key,
    cache_expiration=settings.TASK_CACHE_EXPIRATION,
    refresh_cache=not settings.TASK_CACHE,
)
def load(ctx: Context, ckey: str):
    proxies = ctx.cache.get(ckey)
    out = ctx.load_fragments(proxies, ckey=ckey)
    ctx.log.info("LOADED %d proxies", len(proxies))
    ctx.log.info("OUTPUT: %s", out)
    return out


@task(
    retries=settings.TASK_RETRIES,
    retry_delay_seconds=settings.TASK_RETRY_DELAY,
    cache_key_fn=get_task_cache_key,
    cache_expiration=settings.TASK_CACHE_EXPIRATION,
    refresh_cache=not settings.TASK_CACHE,
)
def transform(ctx: Context, ckey: str) -> str:
    proxies: list[dict[str, Any]] = []
    records = ctx.cache.get(ckey)
    for rec, ix in records:
        for proxy in ctx.config.transform.handle(ctx, rec, ix):
            proxy.datasets = {ctx.dataset}
            proxies.append(proxy.to_dict())
    ctx.log.info("TRANSFORMED %d records", len(records))
    return ctx.cache.set(proxies)


@task(
    retries=settings.TASK_RETRIES,
    retry_delay_seconds=settings.TASK_RETRY_DELAY,
    cache_key_fn=get_task_cache_key,
    cache_expiration=settings.TASK_CACHE_EXPIRATION,
    refresh_cache=not settings.TASK_CACHE,
)
def extract(
    ctx: Context, ckey: str, res: Resolver | None = None
) -> Generator[str, None, None]:
    ctx.log.info("Starting EXTRACT stage ...")
    if res is not None:
        enumerator = enumerate(ctx.config.extract.handle(ctx, res), 1)
    else:
        enumerator = enumerate(ctx.config.extract.handle(ctx), 1)
    batch = []
    ix = 0
    for ix, rec in enumerator:
        batch.append((rec, ix))
        if ix % ctx.config.transform.chunk_size == 0:
            ctx.log.info("extracting record %d ...", ix)
            yield ctx.cache.set(batch)
            batch = []
    if batch:
        yield ctx.cache.set(batch)
    ctx.log.info("EXTRACTED %d records", ix)


@flow(
    name="investigraph-pipeline",
    version=__version__,
    flow_run_name="{ctx.dataset}-{ctx.source.name}",
    task_runner=get_runner_from_env(),
)
def run_pipeline(ctx: Context):
    res = None
    if ctx.config.extract.fetch:
        res = Resolver(source=ctx.source)
        if res.source.is_http:
            res._resolve_http()
        ckey = res.get_cache_key()
    else:
        ckey = ctx.source.uri

    results = []
    for key in extract(ctx, f"extract-{ckey}", res):
        transformed = transform.submit(ctx, key)
        loaded = load.submit(ctx, transformed)
        results.append(loaded)

    return results


@flow(
    name="investigraph",
    version=__version__,
    flow_run_name="{options.flow_name}",
    task_runner=get_runner_from_env(),
)
def run(options: FlowOptions) -> str:
    flow = Flow.from_options(options)
    results = []
    for ix, source in enumerate(flow.config.extract.sources):
        ctx = init_context(config=flow.config, source=source)
        if ix == 0:  # only on first time
            ctx.export_metadata()
            ctx.log.info("INDEX: %s" % ctx.config.load.index_uri)
        results.append(run_pipeline(ctx))

    results = [r.result() for result in results for r in result]

    if flow.should_aggregate:
        results = [aggregate(ctx, results, data_checksum(results))]

    for uri in results:
        ctx.log.info(f"LOADED proxies to `{uri}`")
    return results
