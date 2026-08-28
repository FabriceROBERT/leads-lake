# Airflow + the docker CLI, so BashOperator tasks can launch sibling containers
# (the same `spark-submit jobs/*.py` / `python -m ingestion.*` we run by hand).
FROM apache/airflow:2.10.4

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && rm -rf /var/lib/apt/lists/*
USER airflow
