FROM prefecthq/prefect:2-python3.11

LABEL org.opencontainers.image.title "Investigraph ETL"
LABEL org.opencontainers.image.licenses MIT
LABEL org.opencontainers.image.source https://github.com/investigativedata/investigraph-etl

RUN apt-get update && apt-get -y upgrade
RUN pip install -q -U pip setuptools

RUN apt-get install -y pkg-config libicu-dev
RUN pip install -q --no-binary=:pyicu: pyicu
RUN pip install -q psycopg2-binary
RUN pip install -q asyncpg

COPY investigraph /investigraph/investigraph
COPY setup.py /investigraph/
COPY setup.cfg /investigraph/
COPY VERSION /investigraph/

RUN pip install -q /investigraph

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

RUN mkdir -p /data/datasets
RUN mkdir -p /data/prefect
RUN chown -R 1000:1000 /data

ENV DATA_ROOT=/data
ENV DATASETS_DIR=/data/datasets
ENV PREFECT_HOME=/data/prefect
ENV PREFECT_EXTRA_ENTRYPOINTS=investigraph
ENV PREFECT_SERVER_API_HOST=0.0.0.0
ENV PREFECT_RESULTS_PERSIST_BY_DEFAULT=true
ENV DEBUG=0

USER 1000
WORKDIR /data
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["prefect server start"]
