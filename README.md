# investigraph-prefect

Trying out [prefect.io](https://www.prefect.io/) for ftm pipeline processing

## findings

This current example implementation creates a task for each batch of 1000 csv rows that maps the data to ftm entities and writes them to a sqlite store.

With this approach, the following is achieved:
- parallel and distributed execution

In [this issue](https://github.com/investigativedata/investigraph-prefect/issues/1) we will discuss requirements for the etl process to build upon.

## example dataset

- [European Commission - Meetings with interest representatives](https://data.europa.eu/data/datasets/european-commission-meetings-with-interest-representatives?locale=en)

## run locally

Install pip dependencies (use a virtualenv):

    pip install -r requirements.txt

Execute pipeline:

    python ec_meetings.py

View prefect dashboard:

    prefect server start

Run the reference (standalone) script:

    python ec_meetings_standalone.py


## supported by

[Media Tech Lab Bayern batch #3](https://github.com/media-tech-lab)

<a href="https://www.media-lab.de/en/programs/media-tech-lab">
    <img src="https://raw.githubusercontent.com/media-tech-lab/.github/main/assets/mtl-powered-by.png" width="240" title="Media Tech Lab powered by logo">
</a>
