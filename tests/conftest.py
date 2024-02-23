import subprocess
import sys
import time
from pathlib import Path

import pytest
import requests

from investigraph.model import Config

FIXTURES_PATH = (Path(__file__).parent / "fixtures").absolute()


# https://pawamoy.github.io/posts/local-http-server-fake-files-testing-purposes/
def spawn_and_wait_server():
    process = subprocess.Popen(
        [sys.executable, "-m", "http.server", "-d", FIXTURES_PATH]
    )
    while True:
        try:
            requests.get("http://localhost:8000")
        except Exception:
            time.sleep(1)
        else:
            break
    return process


# credits to pytest-xdist's README
@pytest.fixture(scope="session", autouse=True)
def http_server(tmp_path_factory, worker_id):
    if worker_id == "master":
        # single worker: just run the HTTP server
        process = spawn_and_wait_server()
        yield process
        process.kill()
        process.wait()
        return

    # get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    # try to get a lock
    lock = root_tmp_dir / "lock"
    try:
        lock.mkdir(exist_ok=False)
    except FileExistsError:
        yield  # failed, don't run the HTTP server
        return

    # got the lock, run the HTTP server
    process = spawn_and_wait_server()
    yield process
    process.kill()
    process.wait()


@pytest.fixture(scope="module")
def fixtures_path():
    return FIXTURES_PATH


@pytest.fixture(scope="module")
def ec_meetings():
    return Config.from_uri(FIXTURES_PATH / "ec_meetings" / "config.yml")


@pytest.fixture(scope="module")
def gdho():
    config = Config.from_uri(FIXTURES_PATH / "gdho" / "config.yml")
    for source in config.extract.sources:
        source.uri = "http://localhost:8000/gdho/organizations.csv"
    return config


@pytest.fixture(scope="module")
def eu_authorities():
    config = Config.from_uri(FIXTURES_PATH / "eu_authorities" / "config.yml")
    for source in config.extract.sources:
        source.uri = "http://localhost:8000/all-authorities.csv"
    return config


@pytest.fixture(scope="module")
def ec_meetings_local():
    config = Config.from_uri(FIXTURES_PATH / "ec_meetings" / "config.yml")
    for source in config.extract.sources:
        source.uri = "http://localhost:8000/ec-meetings.xlsx"
    return config
