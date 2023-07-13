from typing import Any

# import html2text
from zavod.util import join_slug

from investigraph.model import Context
from investigraph.util import make_proxy


def handle(ctx: Context, data: dict[str, Any], ix: int):
    body = make_proxy("PublicBody")
    slug = data.pop("URL name")
    body.id = join_slug(ctx.prefix, slug)
    body.add("name", data.pop("Name"))
    body.add("weakAlias", data.pop("Short name"))
    tags = data.pop("Tags").split()
    body.add("keywords", tags)
    body.add("legalForm", tags)
    body.add("website", data.pop("Home page"))
    # body.add("description", html2text.html2text(data.pop("Notes")))
    body.add("sourceUrl", f"https://www.asktheeu.org/en/body/{slug}")
    body.add("jurisdiction", "eu")
    yield body
