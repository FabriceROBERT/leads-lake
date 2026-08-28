"""Batch enrichment via BODACC (annonces légales) -> Bronze.

    python -m ingestion.bodacc --source signal            # ~540 SIREN
    python -m ingestion.bodacc --source siege --resume    # whole parc (slow)

Writes  bronze/source=bodacc/dataset=annonces/data_version=<YYYY-MM-DD>/
        part-000.parquet  (+ manifest.json + _SUCCESS)
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
from ingestion._bodacc import BASE, summarize
from ingestion._s3 import get_fs
from ingestion.recherche_entreprises import _already_done, _siren_list  # reuse helpers

DATASET = "annonces"


async def _fetch(client: httpx.AsyncClient, siren: str, sem: asyncio.Semaphore) -> dict:
    async with sem:
        for attempt in range(4):
            try:
                r = await client.get(
                    BASE,
                    params={
                        "where": f'registre="{siren}"',
                        "limit": 30,
                        "order_by": "dateparution desc",
                    },
                )
                if r.status_code == 429:
                    await asyncio.sleep(2 * (attempt + 1))
                    continue
                if r.status_code != 200:
                    return {"siren": siren}
                return {"siren": siren, **summarize(r.json().get("results", []))}
            except (httpx.HTTPError, ValueError):
                await asyncio.sleep(1.5 * (attempt + 1))
        return {"siren": siren}


async def _run(sirens: list[str], rps: int) -> list[dict]:
    sem = asyncio.Semaphore(rps)
    rows: list[dict] = []
    async with httpx.AsyncClient(timeout=20, headers={"Accept": "application/json"}) as client:
        pending: set = set()
        start = time.monotonic()
        for i, siren in enumerate(sirens):
            target = start + i / rps
            now = time.monotonic()
            if now < target:
                await asyncio.sleep(target - now)
            pending.add(asyncio.create_task(_fetch(client, siren, sem)))
            if len(pending) >= rps * 4:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                rows.extend(t.result() for t in done)
            if i % 500 == 0:
                print(f"  {i}/{len(sirens)}", flush=True)
        if pending:
            done, _ = await asyncio.wait(pending)
            rows.extend(t.result() for t in done)
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["signal", "siege", "all"], default="signal")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--rps", type=int, default=4)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not settings.is_s3:
        raise SystemExit("LAKE_ROOT must be an s3:// URI")

    fs = get_fs()
    base = f"{settings.s3_bucket}/{settings.bronze_prefix}"

    sirens = _siren_list(args.source, args.limit)
    if args.resume:
        done = _already_done(fs, base, source="bodacc", dataset=DATASET)
        sirens = [s for s in sirens if s not in done]
        print(f"resume: {len(done):,} already done")
    print(f"{len(sirens):,} SIREN to enrich (source={args.source}, rps={args.rps})")
    if args.dry_run or not sirens:
        return

    t0 = time.time()
    rows = asyncio.run(_run(sirens, args.rps))
    print(f"fetched {len(rows):,} rows in {time.time() - t0:.0f}s", flush=True)

    df = pd.DataFrame(rows)
    if "bodacc_evenements" in df.columns:
        df["bodacc_evenements"] = df["bodacc_evenements"].apply(
            lambda v: json.dumps(v, ensure_ascii=False) if v else None
        )

    data_version = dt.date.today().isoformat()
    part = f"{base}/source=bodacc/dataset={DATASET}/data_version={data_version}/part-000.parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    with fs.open(part, "wb") as fh:
        fh.write(buf.read())
    prefix = part.rsplit("/", 1)[0]
    with fs.open(f"{prefix}/manifest.json", "w") as fh:
        json.dump(
            {
                "source": "bodacc-datadila.opendatasoft.com",
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
