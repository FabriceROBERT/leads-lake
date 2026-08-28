"""DAG: Bronze -> Silver. Daily (also triggered by ingestion_batch and france_travail).

silver_cabinet must run before silver_offre_emploi (the latter matches offers
against silver/cabinet). Ends by triggering the `gold` DAG.
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from _lib import spark_task

with DAG(
    dag_id="silver",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=3)},
    tags=["silver"],
) as dag:
    cabinet = spark_task("silver_cabinet", "silver_cabinet.py")
    offre_emploi = spark_task("silver_offre_emploi", "silver_offre_emploi.py")

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold", trigger_dag_id="gold", wait_for_completion=False
    )

    cabinet >> offre_emploi >> trigger_gold
