"""DAG: real-time source. Poll France Travail -> Kafka, then drain Kafka -> Bronze.

Runs every 20 min. Idempotent: the poller persists a per-NAF watermark, the
Spark stream keeps a Kafka checkpoint. `stream` uses trigger=availableNow
(process what's there, then stop).
"""

from __future__ import annotations

import pendulum
from airflow import DAG

from _lib import py_task, spark_task

with DAG(
    dag_id="france_travail",
    schedule="*/20 * * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=2)},
    tags=["bronze", "streaming"],
) as dag:
    poll = py_task("poll_offres", "ingestion.france_travail_producer", "--once")
    stream = spark_task("stream_to_bronze", "stream_france_travail.py")

    poll >> stream
