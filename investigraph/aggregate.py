"""
aggregate fragments
"""

from investigraph.util import smart_iter_proxies, smart_write_proxies


def in_memory(in_uri: str, out_uri: str) -> tuple[int, int]:
    fragments = 0
    buffer = {}
    for proxy in smart_iter_proxies(in_uri):
        fragments += 1
        if proxy.id in buffer:
            buffer[proxy.id].merge(proxy)
        else:
            buffer[proxy.id] = proxy
    proxies = smart_write_proxies(out_uri, buffer.values(), serialize=True)
    return fragments, proxies
