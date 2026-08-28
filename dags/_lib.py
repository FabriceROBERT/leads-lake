"""Shared helpers: run a datalake job as a sibling `docker run` from BashOperator."""

from __future__ import annotations

from airflow.operators.bash import BashOperator

NET = "leads-lake-net"
ENVFILE = "/opt/airflow/project.env"
EXTRA_ENV = "-e KAFKA_BOOTSTRAP=kafka:9092 -e KAFKA_TOPIC_FT=france_travail.offres"
SPARK_IMAGE = "leads-lake-spark:3.5.3"
PY_IMAGE = "leads-lake-py:latest"
TILES_IMAGE = "leads-lake-tiles:latest"


def _run(image: str, cmd: str) -> str:
    return f"docker run --rm --network {NET} --env-file {ENVFILE} {EXTRA_ENV} {image} {cmd}".rstrip()


def tiles_task(task_id: str = "build_tiles", **kwargs) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=_run(TILES_IMAGE, ""),
        **kwargs,
    )


def spark_task(task_id: str, job: str, **kwargs) -> BashOperator:
    # pool "heavy" (1 slot) serialises Spark containers so the scheduler VM
    # never runs two at once -> 4g driver is safe.
    kwargs.setdefault("pool", "heavy")
    return BashOperator(
        task_id=task_id,
        bash_command=_run(SPARK_IMAGE, f"/opt/spark/bin/spark-submit --driver-memory 4g jobs/{job}"),
        **kwargs,
    )


def py_task(task_id: str, module: str, args: str = "", **kwargs) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=_run(PY_IMAGE, f"python -m {module} {args}".strip()),
        **kwargs,
    )
