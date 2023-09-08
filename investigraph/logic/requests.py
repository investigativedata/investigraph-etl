"""
hook `requests` into prefect api but let it work like we know:

from investigraph.logic import requests

res = requests.get(url, params={})

"""


from urllib.parse import urlencode

import requests
from prefect import flow, get_run_logger, task
from prefect.tasks import task_input_hash

from investigraph import settings
from investigraph.model.source import Source


def get_request_cache_key(*args, **kwargs) -> str:
    ckey = kwargs.pop("ckey", None)
    if ckey is not None:
        return ckey
    return f"GET#{task_input_hash(*args, **kwargs)}"


@task(
    name="get",
    retries=settings.TASK_RETRIES,
    retry_delay_seconds=settings.TASK_RETRY_DELAY,
    cache_key_fn=get_request_cache_key,
    persist_result=True,
    cache_expiration=settings.TASK_CACHE_EXPIRATION,
    refresh_cache=not settings.TASK_CACHE,
)
def _get(url: str, *args, **kwargs):
    log = get_run_logger()
    kwargs.pop("ckey", None)
    log.info(f"GET {url}?{urlencode(kwargs)}")
    res = requests.get(url, *args, **kwargs)
    assert res.ok
    return res


@flow(name="dispatch-get", flow_run_name="get-{url}", version=settings.VERSION)
def get(url: str, *args, **kwargs):
    """
    Execute `requests.get` within prefect context.
    Do a `head` request beforehand to be able to use cache.
    """
    source = Source(uri=url)
    head = source.head()
    return _get(url, *args, ckey=head.ckey, **kwargs)


# convenience
post = requests.post
head = requests.head
put = requests.put
delete = requests.delete
patch = requests.patch
Response = requests.Response
