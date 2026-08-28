"""DAG: Silver -> Gold. Triggered by the `silver` DAG.

The two jobs are independent (they read Silver, write different Gold tables).
"""

from __future__ import annotations

import pendulum
from airflow import DAG

from _lib import spark_task, tiles_task

with DAG(
    dag_id="gold",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=3)},
    tags=["gold"],
) as dag:
    spark_task("gold_cabinet_zone", "gold_cabinet_zone.py")
    leads = spark_task("gold_leads_scored", "gold_leads_scored.py")
    # vector tiles for the map — depend on the fresh leads_scored
    leads >> tiles_task("build_tiles")
