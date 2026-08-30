"""DAG: contact-crawl, step 1 — resolve `siren -> website domain`.

Monthly. Reads gold/leads_scored, seeds the crawl_frontier (dedicated Postgres,
docker-compose.crawl.yml), then DuckDuckGo-resolves the highest-priority pending
sirens. The crawl worker (step 2, dags/crawl_contacts.py — TODO) drains resolved
rows and extracts phone / e-mail.

See SCORING_V2.md §5 bis.
"""

from __future__ import annotations

import pendulum
from airflow import DAG

from _lib import py_task

with DAG(
    dag_id="crawl_discovery",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=10)},
    tags=["crawl", "contacts"],
) as dag:
    # ~1 req / 2 s against DDG HTML; --limit caps a single run so it stays polite
    # and resumable (priority = lead score -> hottest leads resolved first).
    py_task(
        "discovery",
        "ingestion.crawl_discovery",
        "--limit 6000 --delay 2.0",
    )
