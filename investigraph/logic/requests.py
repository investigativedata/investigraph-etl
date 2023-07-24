"""
hook `requests` into prefect api but let it work like we know:

from investigraph.logic import requests

res = requests.get(url, params={})

"""


import requests
from prefect import flow, get_run_logger, task
from prefect.tasks import task_input_hash

from investigraph import settings


def get_request_cache_key(*args, **kwargs) -> str:
    return f"REQUEST#{task_input_hash(*args, **kwargs)}"


@task(
    retries=settings.TASK_RETRIES,
    retry_delay_seconds=settings.TASK_RETRY_DELAY,
    cache_key_fn=get_request_cache_key,
    persist_result=True,
    cache_expiration=settings.TASK_CACHE_EXPIRATION,
    refresh_cache=not settings.TASK_CACHE,
)
def http(method: str, url: str, *args, **kwargs):
    log = get_run_logger()
    log.info(f"{method.upper()} {url}")
    func = getattr(requests, method)
    return func(url, *args, **kwargs)


@flow(name="requests.head", flow_run_name="head-{url}", version=settings.VERSION)
def head(url: str, *args, **kwargs):
    """
    Execute `requests.get` within prefect context.
    """
    return http("head", url, *args, **kwargs)


@flow(name="requests.get", flow_run_name="get-{url}", version=settings.VERSION)
def get(url: str, *args, **kwargs):
    """
    Execute `requests.get` within prefect context.
    """
    return http("get", url, *args, **kwargs)


@flow(name="requests.post", flow_run_name="post-{url}", version=settings.VERSION)
def post(url: str, *args, **kwargs):
    """
    Execute `requests.post` within prefect context.
    """
    return http("post", url, *args, **kwargs)


Response = requests.Response
