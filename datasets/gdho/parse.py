from typing import Any

from zavod.util import join_slug

from investigraph.model import Context
from investigraph.util import make_proxy

TYPES = {
    "NNGO": "National NGO",
    "INGO": "International NGO",
    "UN": "United Nations",
    "Red Cross/Crescent": "Red Cross/Crescent",
    "": None,
}


def parse(ctx: Context, data: dict[str, Any]):
    proxy = make_proxy("Organization")
    proxy.id = join_slug(ctx.prefix, data.pop("Id"))
    proxy.add("name", data.pop("Name"))
    proxy.add("weakAlias", data.pop("Abbreviated name"))
    proxy.add("legalForm", TYPES[data.pop("Type")])
    proxy.add("website", data.pop("Website"))
    proxy.add("country", data.pop("HQ location"))
    proxy.add("incorporationDate", data.pop("Year founded"))
    proxy.add("dissolutionDate", data.pop("Year closed"))
    proxy.add("sector", data.pop("Sector"))
    proxy.add("sector", data.pop("Religious or secular"))
    proxy.add("sector", data.pop("Religion"))
    yield proxy
