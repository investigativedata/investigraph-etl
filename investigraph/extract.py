"""
Extract sources to iterate objects to dict records
"""

from io import BytesIO
from typing import Generator

import pandas as pd
from pantomime import types

from investigraph.model import SourceResult


def iter_records(res: SourceResult) -> Generator[dict, None, None]:
    if res.mimetype in (types.XLS, types.XLSX):
        df = pd.read_excel(BytesIO(res.content), **res.extract_kwargs).fillna("")
        for _, row in df.iterrows():
            yield dict(row)
        return

    if res.mimetype == types.CSV:
        df = pd.read_csv(BytesIO(res.content), **res.extract_kwargs).fillna("")
        for _, row in df.iterrows():
            yield dict(row)
        return

    raise NotImplementedError("unsupported mimetype: `%s`" % res.mimetype)
