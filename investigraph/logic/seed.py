"""
Seed sources for extraction
"""

from typing import Generator

from fsspec import get_fs_token_paths

from investigraph.model.context import BaseContext
from investigraph.model.source import Source


def handle(ctx: BaseContext) -> Generator[Source, None, None]:
    if ctx.config.seed.glob is not None:
        _, _, uris = get_fs_token_paths(ctx.config.seed.glob)
        for uri in uris:
            yield Source(uri=uri)
