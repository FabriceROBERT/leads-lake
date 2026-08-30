"""DAG: contact-crawl, step 2 — fetch resolved domains for phone / e-mail.

Weekly. Drains the due rows of crawl_frontier (priority = lead score), writes
Bronze `source=crawl/dataset=contacts`, reschedules `next_due_at` by band, then
triggers `silver_contacts`.

Resumable: a run only processes what is due (`--limit`); the rest waits.
See SCORING_V2.md §5 bis.
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from _lib import py_task

with DAG(
    dag_id="crawl_contacts",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=10)},
    tags=["crawl", "contacts"],
) as dag:
    worker = py_task(
        "worker",
        "ingestion.crawl_worker",
        "--limit 4000 --delay 1.5",
    )
    trigger_silver_contacts = TriggerDagRunOperator(
        task_id="trigger_silver_contacts",
        trigger_dag_id="silver_contacts",
        wait_for_completion=False,
    )
    worker >> trigger_silver_contacts
