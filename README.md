# investigraph-prefect

Trying out [prefect.io](https://www.prefect.io/) for ftm pipeline processing

## findings

This current example implementation creates a seperate task for each csv row that maps the data to ftm entities and writes them to a sqlite store.

With this approach, the following is achieved:
- parallel and distributed execution
- granular task reporting
- granular task caching: if a similar csv row was already parsed, prefect will return the cached entities

While this seems to make sense, it is totally inefficient (as in >1.000 times slower plus huge memory and storage overhead) compared to a [test script](./ec_meetings_standalone.py) that just parses the excel files in chunks and emits ftm entities.

Therefore, the per-row task approach seems to be just wrong. Even with other frameworks that would use RabbitMQ or Redis as caching mechanism instead of a SQL-Backend it would still be very inefficient.

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
