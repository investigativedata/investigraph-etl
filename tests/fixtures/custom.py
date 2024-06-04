import csv
import sys
from io import StringIO

import orjson

from investigraph.logic import fetch
from investigraph.model import Context, Source

URL = "http://localhost:8000/all-authorities.csv"


def seed(ctx: Context):
    yield Source(uri=URL, name="all-authorities-csv")


def extract(ctx: Context, *args, **kwargs):
    res = fetch.get(URL)
    reader = csv.DictReader(StringIO(res.text))
    yield from reader


def load(ctx, proxies, *args, **kwargs):
    for proxy in proxies:
        sys.stdout.write(orjson.dumps(proxy).decode())
        return
