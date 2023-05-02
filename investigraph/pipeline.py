"""
The main entrypoint for the prefect flow
"""

import sys
from datetime import timedelta

from prefect import flow, get_run_logger, task
from prefect.tasks import task_input_hash
from prefect_dask.task_runners import DaskTaskRunner

from investigraph.model import Config, get_config


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def dummy(config: Config):
    logger = get_run_logger()
    logger.info(dict(config))


@flow(task_runner=DaskTaskRunner())
def run(dataset: str):
    config = get_config(dataset)
    dummy.submit(config)


if __name__ == "__main__":
    dataset = sys.argv[1]
    run(dataset)
