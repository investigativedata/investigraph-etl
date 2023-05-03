from typing import Any, Generator

import pandas as pd
from fingerprints import generate as fp
from followthemoney.util import join_text, make_entity_id
from nomenklatura.entity import CE
from zavod import Zavod, init_context

URLS = {
    "ec_juncker": "https://www.ec.europa.eu/transparencyinitiative/meetings/dataxlsx.do?name=meetingscommissionrepresentatives1419",
    "ec_leyen": "https://www.ec.europa.eu/transparencyinitiative/meetings/dataxlsx.do?name=meetingscommissionrepresentatives1924",
    "dc_meetings": "https://ec.europa.eu/transparencyinitiative/meetings/dataxlsx.do?name=meetingsdirectorgenerals",
}


def treg_id(context: Zavod, regId: str) -> str:
    return context.make_slug(regId, prefix="eu-tr")


def make_address(context: Zavod, data: dict[str, Any]) -> CE:
    proxy = context.make("Address")
    proxy.add("full", data.pop("Location"))
    proxy.id = context.make_slug(make_entity_id(proxy.caption), prefix="addr")

    context.emit(proxy)
    return proxy


def make_person(context: Zavod, name: str, role: str, body: CE) -> CE:
    proxy = context.make("Person")
    proxy.add("name", name)
    proxy.add("description", role)
    proxy.id = context.make_slug("person", make_entity_id(body.id, fp(name)))

    context.emit(proxy)
    return proxy


def make_organization(context: Zavod, regId: str, name: str | None = None) -> CE:
    proxy = context.make("Organization")
    if fp(name):
        proxy.add("name", name)
    proxy.add("idNumber", regId)
    proxy.id = treg_id(context, regId)
    return proxy


def zip_things(
    context: Zavod, things1: str, things2: str, scream: bool | None = False
) -> Generator[tuple[str, str], None, None]:
    t1 = [t.strip() for t in things1.split(",")]
    t2 = [t.strip() for t in things2.split(",")]
    if len(t1) == len(t2):
        yield from zip(t1, t2)
    elif len(t2) == 1:
        yield things1, things2
    else:
        if scream:
            context.log.error(f"Unable to unzip things: {things1} | {things2}")


def make_organizations(
    context: Zavod, data: dict[str, Any]
) -> Generator[CE, None, None]:
    regIds = data.pop("Transparency register ID")
    orgs = set()
    for name, regId in zip_things(
        context,
        data.pop("Name of interest representative"),
        regIds,
    ):
        org = make_organization(context, regId, name)
        if org.id:
            context.emit(org)
            orgs.add(org.id)
            yield org
    if not orgs:
        # yield only via id
        for regId in regIds.split(","):
            regId = regId.strip()
            org = make_organization(context, regId)
            if org.id:
                context.emit(org)
                orgs.add(org.id)
                yield org


def make_persons(
    context: Zavod, data: dict[str, Any], body: CE
) -> Generator[CE, None, None]:
    for name, role in zip_things(
        context,
        data.pop("Name of EC representative"),
        data.pop("Title of EC representative"),
        scream=True,
    ):
        yield make_person(context, name, role, body)


def make_event(
    context: Zavod, data: dict[str, Any], organizer: CE, involved: list[CE]
) -> CE:
    proxy = context.make("Event")
    date = data.pop("Date of meeting")
    participants = [o for o in make_organizations(context, data)]
    name = f"{date} - {organizer.caption} x {join_text(*[p.first('name') for p in participants])}"
    proxy.add("name", name)
    proxy.add("date", date)
    proxy.add("summary", data.pop("Subject of the meeting"))
    portfolio = data.pop("Portfolio", None)
    if portfolio:
        proxy.add("notes", "Portfolio: " + portfolio)
    address = make_address(context, data)
    proxy.add("location", address.caption)
    proxy.add("address", address.caption)
    proxy.add("addressEntity", address)
    proxy.add("organizer", organizer)
    proxy.add("involved", involved)
    proxy.add("involved", participants)
    proxy.id = context.make_slug(
        "meeting",
        date,
        make_entity_id(organizer.id, *sorted([p.id for p in participants])),
    )

    context.emit(proxy)
    return proxy


def parse_record(context: Zavod, data: dict[str, Any], body: CE):
    involved = [x for x in make_persons(context, data, body)]
    make_event(context, data, body, involved)

    for member in involved:
        rel = context.make("Membership")
        rel.add("organization", body)
        rel.add("member", member)
        rel.add("role", member.get("description"))
        rel.id = context.make_slug("membership", make_entity_id(body.id, member.id))
        context.emit(rel)


def parse_record_ec(context: Zavod, data: dict[str, Any]):
    # meetings of EC representatives
    body = context.make("PublicBody")
    body.add("name", data.pop("Name of cabinet"))
    body.add("jurisdiction", "eu")
    body.id = context.make_slug(fp(body.caption))

    parse_record(context, data, body)
    context.emit(body)


def parse_record_dg(context: Zavod, data: dict[str, Any]):
    # meetings of EC Directors-General
    body = context.make("PublicBody")
    body.add("name", data.pop("Name of DG - full name"))
    acronym = data.pop("Name of DG - acronym")
    body.add("weakAlias", acronym)
    body.add("jurisdiction", "eu")
    body.id = context.make_slug("dg", acronym)

    parse_record(context, data, body)
    context.emit(body)


def parse(context: Zavod):
    for key, url in URLS.items():
        if key.startswith("ec"):
            handler = parse_record_ec
        else:
            handler = parse_record_dg
        data_path = context.fetch_resource(f"{key}.xls", url)
        df = pd.read_excel(data_path, skiprows=1).fillna("")
        ix = 0
        for ix, record in df.iterrows():
            handler(context, dict(record))
            if ix and ix % 1_000 == 0:
                context.log.info("Parse record %d ..." % ix)
        if ix:
            context.log.info("Parsed %d records." % (ix + 1), fp=data_path.name)


if __name__ == "__main__":
    with init_context("config.yml") as context:
        context.export_metadata("export/index.json")
        parse(context)
