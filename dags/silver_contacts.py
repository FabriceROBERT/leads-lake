"""DAG: bronze/source=crawl -> silver/contacts, then trigger `gold`.

Triggered by `crawl_contacts` after each crawl run.
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from _lib import spark_task

with DAG(
    dag_id="silver_contacts",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=3)},
    tags=["silver", "contacts"],
) as dag:
    contacts = spark_task("silver_contacts", "silver_contacts.py")
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold", trigger_dag_id="gold", wait_for_completion=False
    )
    contacts >> trigger_gold
