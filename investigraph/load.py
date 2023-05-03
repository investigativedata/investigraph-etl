"""
Load sources to records iteration
"""

from io import BytesIO
from typing import Generator

import pandas as pd
from pantomime.types import XLSX


def iter_records(mimetype: str, content: bytes) -> Generator[dict, None, None]:
    if mimetype == XLSX:
        df = pd.read_excel(BytesIO(content))
        for _, row in df.iterrows():
            yield dict(row)
        return
    raise NotImplementedError("unsupported mimetype: `%s`" % mimetype)
