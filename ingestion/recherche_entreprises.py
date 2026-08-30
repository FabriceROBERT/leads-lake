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
    """Union of sirens across every part file of every data_version — so a
    multi-day grind resumes cleanly across date rollovers."""
    root = f"{base}/source={source}/dataset={dataset}"
    try:
        dvs = [p for p in fs.ls(root) if "data_version=" in p]
    except FileNotFoundError:
        return set()
    done: set[str] = set()
    for dv in dvs:
        try:
            files = [f for f in fs.ls(dv) if f.endswith(".parquet")]
        except FileNotFoundError:
            continue
        for f in files:
            try:
                df = pd.read_parquet(
                    f"s3://{f}", columns=["siren"], storage_options=settings.storage_options
                )
                done |= {str(s) for s in df["siren"]}
            except Exception:  # noqa: BLE001
                continue
    return done


async def _fetch(client: httpx.AsyncClient, siren: str, sem: asyncio.Semaphore) -> dict | None:
    async with sem:
        for attempt in range(4):
            try:
                r = await client.get(
                    f"{BASE}/search", params={"q": siren, "page": 1, "per_page": 1}
                )
                if r.status_code == 429:
                    await asyncio.sleep(2.0 * (attempt + 1))
                    continue
                if r.status_code != 200:
                    return None
                for res in r.json().get("results", []):
                    if str(res.get("siren")) == siren:
                        return extract(res)
                return {"siren": siren}  # queried, not found — record it so --resume skips
            except (httpx.HTTPError, ValueError):
                await asyncio.sleep(1.5 * (attempt + 1))
        return None


async def _run(sirens: list[str], rps: int, flush_size: int, on_flush) -> int:
    """Dispatch at ~rps/s, hand batches of `flush_size` rows to `on_flush`.
    Returns the total number of kept rows. Hardened: hard per-request timeout,
    a bounded connection pool, and a 90 s escape hatch if the whole in-flight
    batch stalls (the api.gouv endpoint slow-lorises under load)."""
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
                if not done:  # whole batch stalled -> drop it, keep going
                    for t in pending:
                        t.cancel()
                    pending = set()
                    print("  ! batch stalled 90s, dropped", flush=True)
                for t in done:
                    try:
                        r = t.result()
                    except Exception:  # noqa: BLE001
                        r = None
                    if r:
                        rows.append(r)
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
                    r = t.result()
                except Exception:  # noqa: BLE001
                    r = None
                if r:
                    rows.append(r)
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
    ap.add_argument("--resume", action="store_true", help="skip SIREN already written to any part file")
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

    data_version = dt.date.today().isoformat()
    prefix = (
        f"{base}/source=recherche_entreprises/dataset={DATASET}"
        f"/data_version={data_version}"
    )
    try:
        start_k = len([f for f in fs.ls(prefix) if f.endswith(".parquet")])
    except FileNotFoundError:
        start_k = 0

    def _write_part(rows: list[dict], _k=[start_k]) -> None:  # noqa: B006
        df = pd.DataFrame(rows)
        if "dirigeants" in df.columns:
            df["dirigeants"] = df["dirigeants"].apply(
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
                "source": "recherche-entreprises.api.gouv.fr",
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
