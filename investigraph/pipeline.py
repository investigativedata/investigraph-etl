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
from investigraph.model import Config, Source, SourceResult, get_config


@task
def parse(records: Iterable[dict[str, Any]]):
    logger = get_run_logger()
    logger.info("PARSE %d records", len(records))


@task
def fetch(source: Source) -> SourceResult:
    logger = get_run_logger()
    logger.info("FETCH %s", source.uri)
    return fetch_source(source)


@flow(
    name="investigraph-pipeline",
    version=__version__,
    flow_run_name="{config.dataset}-pipeline-{source.name}-{run_id}",
    task_runner=DaskTaskRunner(),
)
def run_pipeline(config: Config, source: Source, run_id: str):
    res = fetch.submit(source)
    res = res.result()
    ix = 0
    batch = []
    for ix, rec in enumerate(iter_records(res.mimetype, res.content)):
        batch.append(rec)
        if ix and ix % 1000 == 0:
            parse.submit(batch)
            batch = []
    if batch:
        parse.submit(batch)


@flow(
    name="investigraph",
    version=__version__,
    flow_run_name="{dataset}-{run_id}",
)
def run(dataset: str, run_id: str):
    config = get_config(dataset)
    for source in config.pipeline.sources:
        run_pipeline(config, source, run_id)


if __name__ == "__main__":
    dataset = sys.argv[1]
    run(dataset, shortuuid.uuid())
