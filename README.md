[![investigraph on pypi](https://img.shields.io/pypi/v/investigraph)](https://pypi.org/project/investigraph/)
[![Python test and package](https://github.com/investigativedata/investigraph-etl/actions/workflows/python.yml/badge.svg)](https://github.com/investigativedata/investigraph-etl/actions/workflows/python.yml)
[![Build docker container](https://github.com/investigativedata/investigraph-etl/actions/workflows/build-docker.yml/badge.svg)](https://github.com/investigativedata/investigraph-etl/actions/workflows/build-docker.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Coverage Status](https://coveralls.io/repos/github/investigativedata/investigraph-etl/badge.svg?branch=main)](https://coveralls.io/github/investigativedata/investigraph-etl?branch=main)
[![MIT License](https://img.shields.io/pypi/l/investigraph)](./LICENSE)

# investigraph

**Research and implementation of an ETL process for a curated and up-to-date public and open-source data catalog of frequently used datasets in investigative journalism.**

Using [prefect.io](https://www.prefect.io/) for ftm pipeline processing

[Documentation](https://investigativedata.github.io/investigraph/)

[Tutorial](https://investigativedata.github.io/investigraph/tutorial/)

## installation

`investigraph` requires at least **Python 3.11**

    pip install investigraph

## example datasets

There is a dedicated [repo](https://github.com/investigativedata/investigraph-datasets) for example datasets built with investigraph.

## deployment

### docker

`docker-compose.yml` for local development / testing, use `docker-compose.prod.yml` as a starting point for a production setup. [More instructions here](https://investigativedata.github.io/investigraph/deployment/)

## run locally

Install app and dependencies (use a virtualenv):

    pip install investigraph

Or, e.g. when using [poetry](https://python-poetry.org/):

    poetry add investigraph

After installation, `investigraph` as a command should be available:

    investigraph --help

Quick run a local dataset definition:

    investigraph run -c ./path/to/config.yml

View prefect dashboard:

    make server

## development

This package is using [poetry](https://python-poetry.org/) for packaging and dependencies management, so first [install it](https://python-poetry.org/docs/#installation).

Clone investigraph repository to a local destination.

Within the root directory, run

    poetry install --with dev

This installs a few development dependencies, including [pre-commit](https://pre-commit.com/) which needs to be registered:

    poetry run pre-commit install

Before creating a commit, this checks for correct code formatting (isort, black) and some other useful stuff (see: `.pre-commit-config.yaml`)

### test

    make test

## supported by

[Media Tech Lab Bayern batch #3](https://github.com/media-tech-lab)

<a href="https://www.media-lab.de/en/programs/media-tech-lab">
    <img src="https://raw.githubusercontent.com/media-tech-lab/.github/main/assets/mtl-powered-by.png" width="240" title="Media Tech Lab powered by logo">
</a>
