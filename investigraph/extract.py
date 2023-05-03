"""
Extract sources to iterate objects to dict records
"""

from io import BytesIO
from typing import Generator

import pandas as pd
from pantomime.types import XLSX

from investigraph.model import SourceResult


def iter_records(res: SourceResult) -> Generator[dict, None, None]:
    if res.mimetype == XLSX:
        df = pd.read_excel(BytesIO(res.content), **res.extract_kwargs).fillna("")
        for _, row in df.iterrows():
            yield dict(row)
        return
    raise NotImplementedError("unsupported mimetype: `%s`" % res.mimetype)
