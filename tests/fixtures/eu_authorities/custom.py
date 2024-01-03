from investigraph.model import Context
from investigraph.types import SDict


def parse_record(ctx: Context, data: SDict, ix: int):
    slug = data.pop("URL name")
    id_ = ctx.make_slug(slug)
    body = ctx.make_proxy("PublicBody", id_)
    body.add("name", data.pop("Name"))
    body.add("weakAlias", data.pop("Short name"))
    tags = data.pop("Tags").split()
    body.add("keywords", tags)
    body.add("legalForm", tags)
    body.add("website", data.pop("Home page"))
    body.add("description", data.pop("Notes"))
    body.add("sourceUrl", f"https://www.asktheeu.org/en/body/{slug}")
    body.add("jurisdiction", "eu")
    yield body
