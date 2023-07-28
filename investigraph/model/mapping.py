from functools import cache

from followthemoney import model
from followthemoney.mapping.query import QueryMapping as _QueryMapping
from ftmq.types import Properties, Schemata
from pydantic import BaseModel, Field


class PropertyMapping(BaseModel):
    column: str | None = None
    columns: list[str] | None = None
    join: str | None = None
    split: str | None = None
    entity: str | None = None
    format: str | None = None
    fuzzy: str | None = None
    required: bool | None = False
    literal: str | None = None
    literals: list[str] | None = None
    template: str | None = None


class EntityMapping(BaseModel):
    key: str | None = None
    keys: list[str] | None = []
    key_literal: str | None = None
    id_column: str | None = None
    schema_: Schemata = Field(..., alias="schema")
    properties: dict[Properties, PropertyMapping] = {}


class QueryMapping(BaseModel):
    entities: dict[str, EntityMapping] | None = {}

    def get_mapping(self) -> _QueryMapping:
        return load_mapping(self)

    def __hash__(self) -> int:
        return hash(repr(self.dict()))


@cache
def load_mapping(mapping: QueryMapping) -> _QueryMapping:
    mapping = mapping.dict(by_alias=True)
    mapping.pop("database", None)
    mapping["csv_url"] = "/dev/null"
    return model.make_mapping(mapping)
