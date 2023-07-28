import numpy as np
import pandas as pd
from followthemoney.mapping import QueryMapping as FtmQueryMapping
from nomenklatura.entity import CompositeEntity

from investigraph.logic.transform import map_record
from investigraph.model.mapping import QueryMapping


def _test_model_mapping(df: pd.DataFrame, mapping: QueryMapping):
    tested = False
    for _, row in df.iterrows():
        row = row.replace(np.nan, None)
        for proxy in map_record(dict(row), mapping):
            assert isinstance(proxy, CompositeEntity)
            tested = True
            break
    assert tested
    return True


def test_model_mappings(eu_authorities, gdho):
    mapping = eu_authorities.transform.queries[0]
    assert isinstance(mapping, QueryMapping)
    assert isinstance(mapping.get_mapping(), FtmQueryMapping)
    df = pd.read_csv(eu_authorities.extract.sources[0].uri)
    assert _test_model_mapping(df, eu_authorities.transform.queries[0])
    df = pd.read_csv(gdho.extract.sources[0].uri, encoding="latin", skiprows=1)
    assert _test_model_mapping(df, gdho.transform.queries[0])
