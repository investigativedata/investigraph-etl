from typing import Generator

from requests import Response


def iter_response(resp: Response) -> Generator[dict, None, None]:
    import ipdb

    ipdb.set_trace()
