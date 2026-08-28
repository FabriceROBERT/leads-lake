"""Batch enrichment via recherche-entreprises.api.gouv.fr -> Bronze.

    python -m ingestion.recherche_entreprises --source signal          # ~540 SIREN, quick
    python -m ingestion.recherche_entreprises --source siege --resume  # whole parc (~11 h @ 6 rps)

Writes  bronze/source=recherche_entreprises/dataset=unites_legales/data_version=<YYYY-MM-DD>/
        part-000.parquet  (+ manifest.json + _SUCCESS)

The Silver join (silver/enrichissement) consumes this partition.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import io
import json
import time

import httpx
import pandas as pd

from app.config.settings import settings
from ingestion._recherche_entreprises import BASE, extract
from ingestion._s3 import get_fs

DATASET = "unites_legales"


def _siren_list(source: str, limit: int | None) -> list[str]:
    so = settings.storage_options
    b = settings.s3_bucket
    if source == "signal":
        df = pd.read_parquet(
            f"s3://{b}/{settings.gold_leads_path}",
            columns=["siren", "nb_offres_90j", "run_date"],
            storage_options=so,
        )
        df = df[df["run_date"].astype(str) == df["run_date"].astype(str).max()]
        sirens = df.loc[df["nb_offres_90j"].fillna(0) > 0, "siren"]
    else:
        df = pd.read_parquet(
            f"s3://{b}/silver/cabinet",
            columns=["siren", "est_siege", "data_version"],
            storage_options=so,
        )
        dv = df["data_version"].astype(str)
        df = df[dv == dv.max()]
        if source == "siege":
            df = df[df["est_siege"] == True]  # noqa: E712
        sirens = df["siren"]
    out = sorted({str(s) for s in sirens if s})
    return out[:limit] if limit else out


def _already_done(
    fs, base: str, source: str = "recherche_entreprises", dataset: str = DATASET
) -> set[str]:
    try:
        parts = sorted(fs.ls(f"{base}/source={source}/dataset={dataset}"))
    except FileNotFoundError:
        return set()
    if not parts:
        return set()
    latest = parts[-1]
    try:
        files = [f for f in fs.ls(latest) if f.endswith(".parquet")]
        if not files:
            return set()
        df = pd.read_parquet(
            f"s3://{files[0]}", columns=["siren"], storage_options=settings.storage_options
        )
        return {str(s) for s in df["siren"]}
    except Exception:  # noqa: BLE001
        return set()


async def _fetch(client: httpx.AsyncClient, siren: str, sem: asyncio.Semaphore) -> dict | None:
    async with sem:
        for attempt in range(4):
            try:
                r = await client.get(
                    f"{BASE}/search", params={"q": siren, "page": 1, "per_page": 1}
                )
                if r.status_code == 429:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                if r.status_code != 200:
                    return None
                for res in r.json().get("results", []):
                    if str(res.get("siren")) == siren:
                        return extract(res)
                return {"siren": siren}  # queried, not found — record it so --resume skips
            except (httpx.HTTPError, ValueError):
                await asyncio.sleep(1.0 * (attempt + 1))
        return None


async def _run(sirens: list[str], rps: int) -> list[dict]:
    sem = asyncio.Semaphore(rps)
    rows: list[dict] = []
    async with httpx.AsyncClient(
        timeout=15, headers={"Accept": "application/json"}
    ) as client:
        pending = set()
        start = time.monotonic()
        for i, siren in enumerate(sirens):
            # crude rate limit: keep dispatch rate <= rps
            target = start + i / rps
            now = time.monotonic()
            if now < target:
                await asyncio.sleep(target - now)
            pending.add(asyncio.create_task(_fetch(client, siren, sem)))
            if len(pending) >= rps * 4:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                rows.extend(r.result() for r in done if r.result())
            if i % 500 == 0:
                print(f"  {i}/{len(sirens)}", flush=True)
        if pending:
            done, _ = await asyncio.wait(pending)
            rows.extend(r.result() for r in done if r.result())
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["signal", "siege", "all"], default="signal")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--rps", type=int, default=6)
    ap.add_argument("--resume", action="store_true", help="skip SIREN already in the latest partition")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not settings.is_s3:
        raise SystemExit("LAKE_ROOT must be an s3:// URI")

    fs = get_fs()
    bucket = settings.s3_bucket
    base = f"{bucket}/{settings.bronze_prefix}"

    sirens = _siren_list(args.source, args.limit)
    if args.resume:
        done = _already_done(fs, base)
        sirens = [s for s in sirens if s not in done]
        print(f"resume: {len(done):,} already done")
    print(f"{len(sirens):,} SIREN to enrich (source={args.source}, rps={args.rps})")
    if args.dry_run or not sirens:
        return

    t0 = time.time()
    rows = asyncio.run(_run(sirens, args.rps))
    print(f"fetched {len(rows):,} rows in {time.time() - t0:.0f}s", flush=True)

    df = pd.DataFrame(rows)
    for col in ("dirigeants",):
        if col in df.columns:
            df[col] = df[col].apply(lambda v: json.dumps(v, ensure_ascii=False) if v else None)

    data_version = dt.date.today().isoformat()
    part = (
        f"{base}/source=recherche_entreprises/dataset={DATASET}"
        f"/data_version={data_version}/part-000.parquet"
    )
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    with fs.open(part, "wb") as fh:
        fh.write(buf.read())
    prefix = part.rsplit("/", 1)[0]
    with fs.open(f"{prefix}/manifest.json", "w") as fh:
        json.dump(
            {
                "source": "recherche-entreprises.api.gouv.fr",
                "dataset": DATASET,
                "data_version": data_version,
                "rows": int(len(df)),
                "selection": args.source,
                "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            },
            fh,
            indent=2,
        )
    with fs.open(f"{prefix}/_SUCCESS", "w") as fh:
        fh.write("")
    print(f"wrote s3://{part}  ({len(df):,} rows)")


if __name__ == "__main__":
    main()
