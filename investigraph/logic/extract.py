"""
Extract sources to iterate objects to dict records
"""

from io import BytesIO, StringIO

import orjson
import pandas as pd
from pantomime import types

from investigraph.model.source import TResponse
from investigraph.types import RecordGenerator

TABULAR = [types.CSV, types.EXCEL, types.XLS, types.XLSX]


def read_pandas(mimetype: str, content: str | bytes, **kwargs) -> pd.DataFrame:
    if isinstance(content, bytes):
        content = BytesIO(content)
    else:
        content = StringIO(content)

    if mimetype in (types.XLS, types.XLSX, types.EXCEL):
        return pd.read_excel(content, **kwargs).fillna("")
    return pd.read_csv(content, **kwargs).fillna("")


def yield_pandas(df: pd.DataFrame) -> RecordGenerator:
    for _, row in df.iterrows():
        row = {k: v if not pd.isna(v) else None for k, v in row.items()}
        yield row


def iter_records(res: TResponse) -> RecordGenerator:
    if res.mimetype in TABULAR:
        kwargs = {**{"dtype": str}, **res.extract_kwargs}
        if res.stream:
            # yield pandas chunks to be able to apply extract_kwargs
            # doesn't work for excel here
            lines = []
            for ix, line in enumerate(res.iter_lines()):
                lines.append(line)
                if ix and ix % 10_000 == 0:
                    content = b"\r".join(lines)
                    df = read_pandas(res.mimetype, content, **kwargs)
                    lines = []
                    if ix == 10_000:  # first chunk
                        # fix initial kwargs for next chunk
                        kwargs["names"] = df.columns
                        kwargs["header"] = 0
                        kwargs.pop("skiprows", None)
                    yield from yield_pandas(df)
            if lines:
                content = b"\r".join(lines)
                df = read_pandas(res.mimetype, content, **kwargs)
                yield from yield_pandas(df)
        else:
            df = read_pandas(res.mimetype, res.content, **kwargs)
            yield from yield_pandas(df)
        return

    if res.mimetype == types.JSON:
        if res.stream:
            for line in res.iter_lines():
                yield orjson.loads(line)
        return

    raise NotImplementedError("unsupported mimetype: `%s`" % res.mimetype)
