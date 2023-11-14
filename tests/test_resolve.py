from io import BytesIO

import pytest
from pantomime.types import CSV, XLSX

from investigraph.exceptions import ImproperlyConfigured
from investigraph.model import Config, Resolver


def test_resolve(eu_authorities: Config, ec_meetings_local: Config):
    tested = False
    for source in ec_meetings_local.extract.sources:
        head = source.head()
        assert not head.can_stream()
        res = Resolver(source=source)
        assert not res.stream
        assert res.mimetype == XLSX
        for chunk in res.iter():
            assert isinstance(chunk, BytesIO)
            tested = True
            break

        assert isinstance(res.get_content(), bytes)
        break
    assert tested

    tested = False
    for source in eu_authorities.extract.sources:
        head = source.head()
        assert head.can_stream()
        res = Resolver(source=source)
        assert res.stream
        assert res.mimetype == CSV
        for chunk in res.iter():
            assert isinstance(chunk, BytesIO)
            tested = True
            break

        break
        with pytest.raises(ImproperlyConfigured):
            res.get_content()
    assert tested
