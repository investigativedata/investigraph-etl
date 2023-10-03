"""
Seed sources for extraction
"""

from typing import Generator

from banal import ensure_list
from fsspec import get_fs_token_paths

from investigraph.model.context import BaseContext
from investigraph.model.source import Source


def handle(ctx: BaseContext) -> Generator[Source, None, None]:
    if ctx.config.seed.glob is not None:
        for glob in ensure_list(ctx.config.seed.glob):
            _, _, uris = get_fs_token_paths(
                glob, storage_options=ctx.config.seed.storage_options
            )
            for uri in uris:
                yield Source(uri=uri)
