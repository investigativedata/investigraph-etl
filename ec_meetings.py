import logging
from datetime import timedelta
from typing import Any, Generator

import pandas as pd
from fingerprints import generate as fp
from followthemoney import model
from followthemoney.util import join_text, make_entity_id
from ftmstore import get_dataset
from nomenklatura.entity import CE
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_dask.task_runners import DaskTaskRunner
from zavod.util import join_slug

URLS = {
    "ec_juncker": "https://www.ec.europa.eu/transparencyinitiative/meetings/dataxlsx.do?name=meetingscommissionrepresentatives1419",
    "ec_leyen": "https://www.ec.europa.eu/transparencyinitiative/meetings/dataxlsx.do?name=meetingscommissionrepresentatives1924",
    "dc_meetings": "https://ec.europa.eu/transparencyinitiative/meetings/dataxlsx.do?name=meetingsdirectorgenerals",
}

log = logging.getLogger(__name__)


def make_proxy(schema: str) -> CE:
    return model.get_proxy({"schema": schema})


def treg_id(regId: str) -> str:
    return join_slug(regId, prefix="eu-tr")


def make_address(data: dict[str, Any]) -> CE:
    proxy = make_proxy("Address")
    proxy.add("full", data.pop("Location"))
    proxy.id = join_slug(make_entity_id(proxy.caption), prefix="addr")
    return proxy


def make_person(name: str, role: str, body: CE) -> CE:
    proxy = make_proxy("Person")
    proxy.add("name", name)
    proxy.add("description", role)
    proxy.id = join_slug("person", make_entity_id(body.id, fp(name)))
    return proxy


def make_organization(regId: str, name: str | None = None) -> CE:
    proxy = make_proxy("Organization")
    if fp(name):
        proxy.add("name", name)
    proxy.add("idNumber", regId)
    proxy.id = treg_id(regId)
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
            log.error(f"Unable to unzip things: {things1} | {things2}")


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
    proxy = make_proxy("Event")
    date = data.pop("Date of meeting")
    participants = [o for o in make_organizations(data)]
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
    proxy.id = join_slug(
        "meeting",
        date,
        make_entity_id(organizer.id, *sorted([p.id for p in participants])),
    )

    yield from participants
    yield address
    yield proxy


def parse_record(data: dict[str, Any], body: CE):
    involved = [x for x in make_persons(data, body)]
    yield from make_event(data, body, involved)
    yield from involved

    for member in involved:
        rel = make_proxy("Membership")
        rel.add("organization", body)
        rel.add("member", member)
        rel.add("role", member.get("description"))
        rel.id = join_slug("membership", make_entity_id(body.id, member.id))
        yield rel


@task(cache_key_fn=task_input_hash)
def parse_record_ec(data: dict[str, Any]):
    # meetings of EC representatives
    body = make_proxy("PublicBody")
    body.add("name", data.pop("Name of cabinet"))
    body.add("jurisdiction", "eu")
    body.id = join_slug(fp(body.caption))

    ds = get_dataset("ec_meetings")
    bulk = ds.bulk()
    for proxy in parse_record(data, body):
        bulk.put(proxy)
    bulk.put(body)
    bulk.flush()


@task(cache_key_fn=task_input_hash)
def parse_record_dg(data: dict[str, Any]):
    # meetings of EC Directors-General
    body = make_proxy("PublicBody")
    body.add("name", data.pop("Name of DG - full name"))
    acronym = data.pop("Name of DG - acronym")
    body.add("weakAlias", acronym)
    body.add("jurisdiction", "eu")
    body.id = join_slug("dg", acronym)

    ds = get_dataset("ec_meetings")
    bulk = ds.bulk()
    for proxy in parse_record(data, body):
        bulk.put(proxy)
    bulk.put(body)
    bulk.flush()


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def load(url: str) -> pd.DataFrame:
    return pd.read_excel(url, skiprows=1)


@flow(name="EC meetings", task_runner=DaskTaskRunner())
def run():
    for key, url in URLS.items():
        future = load.submit(url)
        df = future.result()
        if key.startswith("ec"):
            handler = parse_record_ec
        else:
            handler = parse_record_dg
        for _, row in df.iterrows():
            handler.submit(dict(row))


if __name__ == "__main__":
    run()
