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


async def _run(sirens: list[str], rps: int, flush_size: int, on_flush) -> int:
    sem = asyncio.Semaphore(rps)
    rows: list[dict] = []
    total = 0
    limits = httpx.Limits(max_connections=max(rps * 2, 8), max_keepalive_connections=rps)
    timeout = httpx.Timeout(20.0, connect=10.0, pool=10.0)
    async with httpx.AsyncClient(
        timeout=timeout, limits=limits, headers={"Accept": "application/json"}
    ) as client:
        pending: set = set()
        start = time.monotonic()
        for i, siren in enumerate(sirens):
            target = start + i / rps
            now = time.monotonic()
            if now < target:
                await asyncio.sleep(target - now)
            pending.add(asyncio.create_task(_fetch(client, siren, sem)))
            if len(pending) >= rps * 3:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED, timeout=90
                )
                if not done:
                    for t in pending:
                        t.cancel()
                    pending = set()
                    print("  ! batch stalled 90s, dropped", flush=True)
                for t in done:
                    try:
                        rows.append(t.result())
                    except Exception:  # noqa: BLE001
                        pass
            if len(rows) >= flush_size:
                on_flush(rows)
                total += len(rows)
                rows = []
            if i % 500 == 0:
                print(f"  {i}/{len(sirens)}  (kept {total + len(rows)})", flush=True)
        if pending:
            done, _ = await asyncio.wait(pending, timeout=120)
            for t in done:
                try:
                    rows.append(t.result())
                except Exception:  # noqa: BLE001
                    pass
    if rows:
        on_flush(rows)
        total += len(rows)
    return total


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["signal", "siege", "all"], default="signal")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--rps", type=int, default=4)
    ap.add_argument("--flush", type=int, default=5000, help="rows per part file (checkpoint)")
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

    data_version = dt.date.today().isoformat()
    prefix = f"{base}/source=bodacc/dataset={DATASET}/data_version={data_version}"
    try:
        start_k = len([f for f in fs.ls(prefix) if f.endswith(".parquet")])
    except FileNotFoundError:
        start_k = 0

    def _write_part(rows: list[dict], _k=[start_k]) -> None:  # noqa: B006
        df = pd.DataFrame(rows)
        if "bodacc_evenements" in df.columns:
            df["bodacc_evenements"] = df["bodacc_evenements"].apply(
                lambda v: json.dumps(v, ensure_ascii=False) if v else None
            )
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        part = f"{prefix}/part-{_k[0]:03d}.parquet"
        with fs.open(part, "wb") as fh:
            fh.write(buf.read())
        print(f"  flushed {len(df):,} -> s3://{part}", flush=True)
        _k[0] += 1

    t0 = time.time()
    total = asyncio.run(_run(sirens, args.rps, args.flush, _write_part))
    print(f"kept {total:,} rows in {time.time() - t0:.0f}s", flush=True)

    with fs.open(f"{prefix}/manifest.json", "w") as fh:
        json.dump(
            {
                "source": "bodacc-datadila.opendatasoft.com",
                "dataset": DATASET,
                "data_version": data_version,
                "rows": int(total),
                "selection": args.source,
                "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            },
            fh,
            indent=2,
        )
    with fs.open(f"{prefix}/_SUCCESS", "w") as fh:
        fh.write("")
    print(f"done: {total:,} rows across part files under s3://{prefix}")


if __name__ == "__main__":
    main()
