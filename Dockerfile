FROM ghcr.io/investigativedata/ftm-docker:main

LABEL org.opencontainers.image.title "Investigraph ETL"
LABEL org.opencontainers.image.licenses MIT
LABEL org.opencontainers.image.source https://github.com/investigativedata/investigraph-etl

RUN apt-get update && apt-get -y upgrade

COPY investigraph /investigraph/investigraph
COPY setup.py /investigraph/
COPY setup.cfg /investigraph/
COPY VERSION /investigraph/


RUN pip install -q -U pip setuptools
RUN pip install -q /investigraph

COPY datasets /investigraph/datasets
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENV DATASETS_DIR=/investigraph/datasets
ENV PREFECT_HOME=/data/prefect
ENV PREFECT_EXTRA_ENTRYPOINTS=investigraph
ENV PREFECT_SERVER_API_HOST=0.0.0.0

WORKDIR /investigraph
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["prefect server start"]
