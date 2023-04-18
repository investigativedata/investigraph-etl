# investigraph-prefect

Trying out [prefect.io](https://www.prefect.io/) for ftm pipeline processing

## example dataset

- [European Commission - Meetings with interest representatives](https://data.europa.eu/data/datasets/european-commission-meetings-with-interest-representatives?locale=en)

## run locally

Install pip dependencies (use a virtualenv):

    pip install -r requirements.txt

Execute pipeline:

    python ec_meetings.py

View prefect dashboard:

    prefect server start



## supported by

[Media Tech Lab Bayern batch #3](https://github.com/media-tech-lab)

<a href="https://www.media-lab.de/en/programs/media-tech-lab">
    <img src="https://raw.githubusercontent.com/media-tech-lab/.github/main/assets/mtl-powered-by.png" width="240" title="Media Tech Lab powered by logo">
</a>
