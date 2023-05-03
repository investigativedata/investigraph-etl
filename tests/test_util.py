from nomenklatura.entity import CompositeEntity

from investigraph import util


def test_util_smart_iter_proxies():
    ix = 0
    for proxy in util.smart_iter_proxies(
        "https://data.ftm.store/ec_meetings/entities.ftm.json"
    ):
        assert isinstance(proxy, CompositeEntity)
        ix = 1
        break
    assert ix == 1
