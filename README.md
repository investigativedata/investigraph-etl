[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit) [![Coverage Status](https://coveralls.io/repos/github/investigativedata/investigraph-etl/badge.svg?branch=main)](https://coveralls.io/github/investigativedata/investigraph-etl?branch=main)

# investigraph

**Research and implementation of an ETL process for a curated and up-to-date public and open-source data catalog of frequently used datasets in investigative journalism.**

Using [prefect.io](https://www.prefect.io/) for ftm pipeline processing

[Documentation](https://investigativedata.github.io/investigraph/)

[Tutorial](https://investigativedata.github.io/investigraph/tutorial/)

## installation

    pip install investigraph

## example datasets

There is a dedicated [repo](https://github.com/investigativedata/investigraph-datasets) for example datasets that can be used as a [Block](https://docs.prefect.io/2.10.11/concepts/blocks/) within the prefect.io deployment.

## deployment

### docker

`docker-compose.yml` for local development / testing, use `docker-compose.prod.yml` as a starting point for a production setup. [More instructions here](https://investigativedata.github.io/investigraph/deployment/)

## run locally

Clone repo first.

Install app and dependencies (use a virtualenv):

    pip install -e .

After installation, `investigraph` as a command should be available:

    investigraph --help

Quick run a local dataset definition:

    investigraph run <dataset_name> -c ./path/to/config.yml

Register a local datasets block:

    investigraph add-block -b local-file-system/investigraph-local -u ./datasets

Register github datasets block:

    investigraph add-block -b github/investigraph-datasets -u https://github.com/investigativedata/investigraph-datasets.git

Run a dataset pipeline from a dataset defined in a registered block:

    investigraph run ec_meetings

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
