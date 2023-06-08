from typing import Any, Generator

from fingerprints import generate as fp
from followthemoney.util import join_text, make_entity_id
from nomenklatura.entity import CE
from zavod.util import join_slug

from investigraph.model import Context
from investigraph.util import make_proxy


def treg_id(regId: str) -> str:
    return join_slug(regId, prefix="eu-tr")


def make_address(data: dict[str, Any]) -> CE:
    proxy = make_proxy("Address")
    proxy.id = join_slug(make_entity_id(proxy.caption), prefix="addr")
    proxy.add("full", data.pop("Location"))
    return proxy


def make_person(name: str, role: str, body: CE) -> CE:
    proxy = make_proxy("Person")
    proxy.id = join_slug("person", make_entity_id(body.id, fp(name)))
    proxy.add("name", name)
    proxy.add("description", role)
    return proxy


def make_organization(regId: str, name: str | None = None) -> CE:
    proxy = make_proxy("Organization")
    proxy.id = treg_id(regId)
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


def make_organizations(data: dict[str, Any]) -> Generator[CE, None, None]:
    regIds = data.pop("Transparency register ID")
    orgs = False
    for name, regId in zip_things(
        data.pop("Name of interest representative"),
        regIds,
    ):
        org = make_organization(regId, name)
        if org.id:
            orgs = True
            yield org
    if not orgs:
        # yield only via id
        for regId in regIds.split(","):
            regId = regId.strip()
            org = make_organization(regId)
            if org.id:
                yield org


def make_persons(data: dict[str, Any], body: CE) -> Generator[CE, None, None]:
    for name, role in zip_things(
        data.pop("Name of EC representative"),
        data.pop("Title of EC representative"),
        scream=True,
    ):
        yield make_person(name, role, body)


def make_event(
    data: dict[str, Any], organizer: CE, involved: list[CE]
) -> Generator[CE, None, None]:
    date = data.pop("Date of meeting")
    participants = [o for o in make_organizations(data)]
    proxy = make_proxy("Event")
    proxy.id = join_slug(
        "meeting",
        date,
        make_entity_id(organizer.id, *sorted([p.id for p in participants])),
    )
    name = f"{date} - {organizer.caption} x {join_text(*[p.first('name') for p in participants])}"
    proxy.add("name", name)
    proxy.add("date", date)
    proxy.add("summary", data.pop("Subject of the meeting"))
    portfolio = data.pop("Portfolio", None)
    if portfolio:
        proxy.add("notes", "Portfolio: " + portfolio)
    address = make_address(data)
    proxy.add("location", address.caption)
    proxy.add("address", address.caption)
    proxy.add("addressEntity", address)
    proxy.add("organizer", organizer)
    proxy.add("involved", involved)
    proxy.add("involved", participants)

    yield from participants
    yield address
    yield proxy


def parse_record(data: dict[str, Any], body: CE):
    involved = [x for x in make_persons(data, body)]
    yield from make_event(data, body, involved)
    yield from involved

    for member in involved:
        rel = make_proxy("Membership")
        rel.id = join_slug("membership", make_entity_id(body.id, member.id))
        rel.add("organization", body)
        rel.add("member", member)
        rel.add("role", member.get("description"))
        yield rel


def parse_record_ec(data: dict[str, Any]):
    # meetings of EC representatives
    body = make_proxy("PublicBody")
    body.id = join_slug(fp(body.caption))
    body.add("name", data.pop("Name of cabinet"))
    body.add("jurisdiction", "eu")

    yield body
    yield from parse_record(data, body)


def parse_record_dg(data: dict[str, Any]):
    # meetings of EC Directors-General
    acronym = data.pop("Name of DG - acronym")
    body = make_proxy("PublicBody")
    body.id = join_slug("dg", acronym)
    body.add("name", data.pop("Name of DG - full name"))
    body.add("weakAlias", acronym)
    body.add("jurisdiction", "eu")

    yield body
    yield from parse_record(data, body)


def parse(ctx: Context, data: dict[str, Any]):
    if ctx.source.name.startswith("ec"):
        handler = parse_record_ec
    else:
        handler = parse_record_dg
    yield from handler(data)
