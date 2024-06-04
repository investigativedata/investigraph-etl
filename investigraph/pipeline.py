"""
The main entrypoint for the prefect flow
"""

from collections.abc import Generator
from datetime import datetime
from functools import cache
from typing import Any, Type

import orjson
from anystore.io import smart_open
from anystore.util import make_data_checksum
from ftmq.model.coverage import DatasetStats
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect_dask import DaskTaskRunner
from prefect_ray import RayTaskRunner

from investigraph import __version__, settings
from investigraph.model.context import BaseContext, Context
from investigraph.model.flow import Flow, FlowOptions
from investigraph.model.resolver import Resolver


@cache
def get_runner_from_env() -> (
    Type[ConcurrentTaskRunner] | Type[DaskTaskRunner] | Type[RayTaskRunner]
):
    if settings.TASK_RUNNER == "dask":
        return DaskTaskRunner
    if settings.TASK_RUNNER == "ray":
        return RayTaskRunner
    return ConcurrentTaskRunner


def get_task_cache_key(_, params) -> str:
    return params["ckey"]


@task(
    retries=settings.TASK_RETRIES,
    retry_delay_seconds=settings.TASK_RETRY_DELAY,
    cache_key_fn=get_task_cache_key,
    cache_expiration=settings.TASK_CACHE_EXPIRATION,
    refresh_cache=not settings.TASK_CACHE,
    cache_result_in_memory=False,
)
def aggregate(ctx: Context, results: list[str], ckey: str) -> DatasetStats:
    fragments, stats = ctx.aggregate(ctx, results)
    ctx.log.info("AGGREGATED %d fragments to %d proxies", fragments, stats.entity_count)
    ctx.log.info("OUTPUT: %s", ctx.config.load.entities_uri)
    return stats


@task(
    retries=settings.TASK_RETRIES,
    retry_delay_seconds=settings.TASK_RETRY_DELAY,
    cache_key_fn=get_task_cache_key,
    cache_expiration=settings.TASK_CACHE_EXPIRATION,
    refresh_cache=not settings.TASK_CACHE or not settings.LOAD_CACHE,
    cache_result_in_memory=False,
)
def load(ctx: Context, ckey: str) -> str | None:
    proxies = ctx.cache.get(ckey)
    if proxies is None:
        ctx.log.warn(f"No proxies found for cache key `{ckey}`")
        return
    out = ctx.load_fragments(proxies, ckey=ckey)
    ctx.log.info("LOADED %d proxies", len(proxies))
    ctx.log.info("OUTPUT: %s", out)
    return out


@task(
    retries=settings.TASK_RETRIES,
    retry_delay_seconds=settings.TASK_RETRY_DELAY,
    cache_key_fn=get_task_cache_key,
    cache_expiration=settings.TASK_CACHE_EXPIRATION,
    refresh_cache=not settings.TASK_CACHE or not settings.TRANSFORM_CACHE,
    cache_result_in_memory=False,
)
def transform(ctx: Context, ckey: str) -> str | None:
    proxies: list[dict[str, Any]] = []
    records = ctx.cache.get(ckey)
    if records is None:
        ctx.log.warn(f"No records found for cache key `{ckey}`")
        return
    for rec, ix in records:
        try:
            for proxy in ctx.config.transform.handle(ctx, rec, ix):
                proxies.append(proxy.to_dict())
        except Exception as e:
            ctx.log.error(f"{e.__class__.__name__}: {e}")
    ctx.log.info("TRANSFORMED %d records", len(records))
    return ctx.cache.set(proxies)


def extract(
    ctx: Context, ckey: str, res: Resolver | None = None
) -> Generator[str, None, None]:
    ctx.log.info("Starting EXTRACT stage ...")
    if settings.TASK_CACHE or settings.EXTRACT_CACHE:
        cached_result = ctx.cache.get(ckey)
        if cached_result is not None:
            ctx.log.info("EXTRACT complete (CACHED)")
            yield from cached_result
            return
    if res is not None:
        enumerator = enumerate(ctx.config.extract.handle(ctx, res), 1)
    else:
        enumerator = enumerate(ctx.config.extract.handle(ctx), 1)
    batch = []
    batch_keys = []
    ix = 0
    for ix, rec in enumerator:
        batch.append((rec, ix))
        if ix % ctx.config.transform.chunk_size == 0:
            ctx.log.info("extracting record %d ...", ix)
            batch_key = ctx.cache.set(batch)
            batch_keys.append(batch_key)
            yield batch_key
            batch = []
    if batch:
        batch_key = ctx.cache.set(batch)
        batch_keys.append(batch_key)
        yield batch_key
    ctx.cache.set(batch_keys, ckey)
    ctx.log.info("EXTRACTED %d records", ix)


@flow(
    name="investigraph-extract",
    version=__version__,
    flow_run_name="{ctx.source.name}",
    task_runner=get_runner_from_env(),
    cache_result_in_memory=False,
)
def run_pipeline(ctx: Context, extract_only: bool | None = False) -> list[Any]:
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
        if extract_only:
            with smart_open(ctx.config.extract.records_uri, mode="ba") as f:
                for record, _ in ctx.cache.get(key):
                    f.write(orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE))
            return results

        transformed = transform.submit(ctx, key)
        loaded = load.submit(ctx, transformed)
        results.append(loaded)

    return results


@flow(
    name="investigraph",
    version=__version__,
    flow_run_name="{options.flow_name}",
    task_runner=get_runner_from_env(),
    cache_result_in_memory=False,
)
def run(options: FlowOptions) -> Flow:
    flow = Flow.from_options(options)
    results = []
    ctx = BaseContext.from_config(flow.config)

    for ix, run_ctx in enumerate(ctx.from_sources()):
        if ix == 0:  # only on first time
            ctx.export_metadata()
            ctx.log.info("INDEX: %s" % ctx.config.load.index_uri)
        results.extend(run_pipeline(run_ctx, extract_only=flow.extract_only))

    if flow.config.aggregate:
        fragments = [r.result() for r in results]
        res = aggregate.submit(ctx, fragments, make_data_checksum(fragments))
        ctx.config.dataset.apply_stats(res.result())
        ctx.export_metadata()
        ctx.log.info("INDEX (updated with coverage): %s" % ctx.config.load.index_uri)

    flow.end = datetime.utcnow()
    fragment_uris = [r.result() for r in results]
    flow.fragment_uris = filter(lambda x: x is not None, fragment_uris)
    return flow
