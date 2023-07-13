from typing import Generator

from fingerprints import generate as fp
from followthemoney.util import join_text, make_entity_id
from nomenklatura.entity import CE

from investigraph.model import Context
from investigraph.types import SDict


def make_address(ctx: Context, data: SDict) -> CE:
    proxy = ctx.make_proxy("Address")
    proxy.id = ctx.make_id(proxy.caption, prefix="addr")
    proxy.add("full", data.pop("Location"))
    return proxy


def make_person(ctx: Context, name: str, role: str, body: CE) -> CE:
    proxy = ctx.make_proxy("Person")
    proxy.id = ctx.make_slug("person", make_entity_id(body.id, fp(name)))
    proxy.add("name", name)
    proxy.add("description", role)
    return proxy


def make_organization(ctx: Context, regId: str, name: str | None = None) -> CE:
    proxy = ctx.make_proxy("Organization")
    proxy.id = ctx.make_slug(regId, prefix="eu-tr")
    if fp(name):
        proxy.add("name", name)
    proxy.add("idNumber", regId)
    return proxy


def zip_things(
    things1: str, things2: str, scream: bool | None = False
) -> Generator[tuple[str, str], None, None]:
    t1 = [t.strip() for t in things1.split(",")]
    t2 = [t.strip() for t in things2.split(",")]
    if len(t1) == len(t2):
        yield from zip(t1, t2)
    elif len(t2) == 1:
        yield things1, things2
    else:
        if scream:
            raise Exception
            # log.error(f"Unable to unzip things: {things1} | {things2}")


def make_organizations(ctx: Context, data: SDict) -> Generator[CE, None, None]:
    regIds = data.pop("Transparency register ID")
    orgs = False
    for name, regId in zip_things(
        data.pop("Name of interest representative"),
        regIds,
    ):
        org = make_organization(ctx, regId, name)
        if org.id:
            orgs = True
            yield org
    if not orgs:
        # yield only via id
        for regId in regIds.split(","):
            regId = regId.strip()
            org = make_organization(ctx, regId)
            if org.id:
                yield org


def make_persons(ctx: Context, data: SDict, body: CE) -> Generator[CE, None, None]:
    for name, role in zip_things(
        data.pop("Name of EC representative"),
        data.pop("Title of EC representative"),
        scream=True,
    ):
        yield make_person(ctx, name, role, body)


def make_event(
    ctx: Context, data: SDict, organizer: CE, involved: list[CE]
) -> Generator[CE, None, None]:
    date = data.pop("Date of meeting")
    participants = [o for o in make_organizations(ctx, data)]
    proxy = ctx.make_proxy("Event")
    proxy.id = ctx.make_slug(
        "meeting",
        date,
        make_entity_id(organizer.id, *sorted([p.id for p in participants])),
    )
    label = join_text(*[p.first("name") for p in participants])
    name = f"{date} - {organizer.caption} x {label}"
    proxy.add("name", name)
    proxy.add("date", date)
    proxy.add("summary", data.pop("Subject of the meeting"))
    portfolio = data.pop("Portfolio", None)
    if portfolio:
        proxy.add("notes", "Portfolio: " + portfolio)
    address = make_address(ctx, data)
    proxy.add("location", address.caption)
    proxy.add("address", address.caption)
    proxy.add("addressEntity", address)
    proxy.add("organizer", organizer)
    proxy.add("involved", involved)
    proxy.add("involved", participants)

    yield from participants
    yield address
    yield proxy


def parse_record(ctx: Context, data: SDict, body: CE):
    involved = [x for x in make_persons(ctx, data, body)]
    yield from make_event(ctx, data, body, involved)
    yield from involved

    for member in involved:
        rel = ctx.make_proxy("Membership")
        rel.id = ctx.make_slug("membership", make_entity_id(body.id, member.id))
        rel.add("organization", body)
        rel.add("member", member)
        rel.add("role", member.get("description"))
        yield rel


def parse_record_ec(ctx: Context, data: SDict):
    # meetings of EC representatives
    body = ctx.make_proxy("PublicBody")
    body.id = ctx.make_slug(fp(body.caption))
    body.add("name", data.pop("Name of cabinet"))
    body.add("jurisdiction", "eu")

    yield body
    yield from parse_record(ctx, data, body)


def parse_record_dg(ctx: Context, data: SDict):
    # meetings of EC Directors-General
    acronym = data.pop("Name of DG - acronym")
    body = ctx.make_proxy("PublicBody")
    body.id = ctx.make_slug("dg", acronym)
    body.add("name", data.pop("Name of DG - full name"))
    body.add("weakAlias", acronym)
    body.add("jurisdiction", "eu")

    yield body
    yield from parse_record(ctx, data, body)


def handle(ctx: Context, data: SDict, ix: int):
    if ctx.source.name.startswith("ec"):
        handler = parse_record_ec
    else:
        handler = parse_record_dg
    yield from handler(ctx, data)
