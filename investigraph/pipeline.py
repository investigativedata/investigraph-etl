"""
The main entrypoint for the prefect flow
"""

import sys

import shortuuid
from prefect import flow, get_run_logger, task
from prefect_dask.task_runners import DaskTaskRunner

from investigraph import __version__
from investigraph.model import Config, Source, get_config


@task
def fetch(uri: str):
    logger = get_run_logger()
    logger.info("FETCH %s", uri)


@flow(
    name="investigraph-pipeline",
    version=__version__,
    flow_run_name="{config.dataset}-pipeline-{source.name}-{run_id}",
    task_runner=DaskTaskRunner(),
)
def run_pipeline(config: Config, source: Source, run_id: str):
    logger = get_run_logger()
    logger.info(dict(source))
    fetch.submit(source.uri)


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
