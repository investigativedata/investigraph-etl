FROM prefecthq/prefect:3-python3.12

LABEL org.opencontainers.image.title="Investigraph ETL"
LABEL org.opencontainers.image.licenses=MIT
LABEL org.opencontainers.image.source=https://github.com/investigativedata/investigraph-etl

RUN apt-get update && apt-get -y upgrade
RUN pip install -q -U pip setuptools

RUN apt-get install -y pkg-config libicu-dev
RUN apt-get install -y libleveldb-dev
RUN pip install -q --no-binary=:pyicu: pyicu
RUN pip install -q psycopg2-binary
RUN pip install -q asyncpg

COPY investigraph /investigraph/investigraph
COPY setup.py /investigraph/
COPY pyproject.toml /investigraph/
COPY VERSION /investigraph/
COPY README.md /investigraph/

RUN pip install -q /investigraph

RUN mkdir -p /data/.prefect
RUN chown -R 1000:1000 /data

ENV INVESTIGRAPH_DATA_ROOT=/data
ENV PREFECT_HOME=/data/.prefect
ENV PREFECT_EXTRA_ENTRYPOINTS=investigraph
ENV PREFECT_SERVER_API_HOST=0.0.0.0
ENV PREFECT_RESULTS_PERSIST_BY_DEFAULT=true
ENV DEBUG=0

USER 1000
WORKDIR /data
CMD ["prefect server start"]
