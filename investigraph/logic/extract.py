"""
Extract sources to iterate objects to dict records
"""

from io import BytesIO, StringIO

import pandas as pd
from pantomime import types

from investigraph.model import SourceResponse
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
        yield dict(row)


def iter_records(res: SourceResponse) -> RecordGenerator:
    if res.mimetype in TABULAR:
        kwargs = {**{"dtype": str}, **res.extract_kwargs}
        if res.is_stream:
            # yield pandas chunks to be able to apply extract_kwargs
            # doesn't work for excel here
            lines = []
            for ix, line in enumerate(res.response.iter_lines()):
                lines.append(line)
                if ix and ix % 10_000 == 0:
                    content = b"\n".join(lines)
                    df = read_pandas(res.mimetype, content, **kwargs)
                    lines = []
                    if ix == 10_000:  # first chunk
                        # fix initial kwargs for next chunk
                        kwargs["names"] = df.columns
                        kwargs["header"] = 0
                        kwargs.pop("skiprows")  # FIXME what else?
                    yield from yield_pandas(df)
            if lines:
                content = b"\n".join(lines)
                df = read_pandas(res.mimetype, content, **kwargs)
                yield from yield_pandas(df)
        else:
            df = read_pandas(res.mimetype, res.response.content, **kwargs)
            yield from yield_pandas(df)
        return

    raise NotImplementedError("unsupported mimetype: `%s`" % res.mimetype)
