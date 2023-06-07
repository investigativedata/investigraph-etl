# investigraph-prefect

Trying out [prefect.io](https://www.prefect.io/) for ftm pipeline processing

## status

This current example implementation creates a task for each batch of 1000 csv rows that maps the data to ftm entities and writes them to an output file.

With this approach, the following is achieved:
- parallel and distributed execution
- task state monitoring via prefect ui

In [this issue](https://github.com/investigativedata/investigraph-prefect/issues/1) we will discuss requirements for the etl process to build upon.

## example datasets

There is a dedicated [repo](https://github.com/investigativedata/investigraph-datasets) for example datasets that can be used as a [Block](https://docs.prefect.io/2.10.11/concepts/blocks/) within the prefect.io deployment.

## run locally

Clone repo first.

Install app and dependencies (use a virtualenv):

    pip install -e .

After installation, `investigraph` as a command should be available:

    investigraph --help

Register default datasets block:

    investigraph add-block -b github/investigraph-datasets -r https://github.com/investigativedata/investigraph-datasets.git

Run a dataset pipeline:

    investigraph run ec_meetings

View prefect dashboard:

    make server

## test

    make install
    make test

## supported by

[Media Tech Lab Bayern batch #3](https://github.com/media-tech-lab)

<a href="https://www.media-lab.de/en/programs/media-tech-lab">
    <img src="https://raw.githubusercontent.com/media-tech-lab/.github/main/assets/mtl-powered-by.png" width="240" title="Media Tech Lab powered by logo">
</a>
