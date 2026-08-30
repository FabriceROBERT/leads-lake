"""DAG: batch ingestion into Bronze (SIRENE + INSEE nomenclature), monthly.

Idempotent: each job skips a data_version already carrying `_SUCCESS`.
Ends by triggering the `silver` DAG.
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from _lib import py_task

with DAG(
    dag_id="ingestion_batch",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["bronze", "batch"],
) as dag:
    sirene_ul = py_task(
        "sirene_unite_legale",
        "ingestion.sirene_stock",
        "--dataset stock_unite_legale --format parquet",
    )
    sirene_etab = py_task(
        "sirene_etablissement",
        "ingestion.sirene_stock",
        "--dataset stock_etablissement --format parquet",
    )
    insee_cj = py_task("insee_categories_juridiques", "ingestion.insee_categories_juridiques")

    # Financials: bulk file (data.economie.gouv.fr / ratios_inpi_bce). One download,
    # no per-siren calls -> no WAF ban. Primary source for ca / resultat_net.
    ratios_fin = py_task("ratios_financiers", "ingestion.ratios_financiers")

    # BODACC "why now" events, whole parc. Checkpointed (part files every --flush),
    # --resume skips sirens already written.
    enrich_bodacc = py_task(
        "bodacc",
        "ingestion.bodacc",
        "--source siege --resume --rps 4 --flush 5000",
    )

    # recherche-entreprises: display-only fields (dirigeants, catégorie, Qualiopi).
    # Off the critical path — that API bans bulk callers; when blocked the script
    # writes 0 rows and exits 0.
    enrich_rne = py_task(
        "recherche_entreprises",
        "ingestion.recherche_entreprises",
        "--source siege --resume --rps 2 --flush 5000",
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver", trigger_dag_id="silver", wait_for_completion=False
    )

    sirene = [sirene_ul, sirene_etab, insee_cj]
    sirene >> ratios_fin >> trigger_silver
    sirene >> enrich_bodacc >> trigger_silver
    sirene >> enrich_rne  # best-effort, not upstream of silver
