FROM prefecthq/prefect:2-python3.11

LABEL org.opencontainers.image.title "Investigraph ETL"
LABEL org.opencontainers.image.licenses MIT
LABEL org.opencontainers.image.source https://github.com/investigativedata/investigraph-etl

RUN apt-get update && apt-get -y upgrade
RUN pip install -q -U pip setuptools

RUN apt-get install -y pkg-config libicu-dev
RUN pip install -q --no-binary=:pyicu: pyicu

COPY investigraph /investigraph/investigraph
COPY setup.py /investigraph/
COPY setup.cfg /investigraph/
COPY VERSION /investigraph/

RUN pip install -q /investigraph

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENV DATASETS_DIR=/investigraph/datasets
ENV PREFECT_HOME=/data/prefect
ENV PREFECT_EXTRA_ENTRYPOINTS=investigraph
ENV PREFECT_SERVER_API_HOST=0.0.0.0

WORKDIR /investigraph
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["prefect server start"]
