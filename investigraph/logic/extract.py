"""
Extract sources to iterate objects to dict records
"""

import numpy as np
import pandas as pd
from pantomime import types
from runpandarun.io import guess_handler_from_mimetype

from investigraph.model.context import Context
from investigraph.model.resolver import Resolver
from investigraph.types import RecordGenerator

TABULAR = [types.CSV, types.EXCEL, types.XLS, types.XLSX]
JSON = types.JSON


def yield_pandas(df: pd.DataFrame) -> RecordGenerator:
    for _, row in df.iterrows():
        yield dict(row.replace(np.nan, None))


def extract_pandas(
    resolver: Resolver, chunk_size: int | None = 10_000
) -> RecordGenerator:
    play = resolver.source.pandas
    play.read.uri = resolver.source.uri
    if play.read.handler is None:
        play.read.handler = f"read_{guess_handler_from_mimetype(resolver.mimetype)}"
    for ix, chunk in enumerate(resolver.iter(chunk_size)):
        df = play.read.handle(chunk)
        if resolver.mimetype == types.CSV:
            if ix == 0:  # first chunk
                play.read.options["names"] = df.columns
                play.read.options["skiprows"] = 0
                play.read.options["header"] = 0
        df = play.run(df)
        yield from yield_pandas(df)


# entrypoint
def handle(ctx: Context, resolver: Resolver, *args, **kwargs) -> RecordGenerator:
    yield from extract_pandas(resolver, *args, **kwargs)
