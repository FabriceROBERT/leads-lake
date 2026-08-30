"""Contact-crawl, step 1: resolve `siren -> website domain` for the Gold parc.

    python -m ingestion.crawl_discovery --limit 2000            # top 2000 pending by score
    python -m ingestion.crawl_discovery --seed-only             # just refresh the frontier

Flow:
  1. ensure crawl_frontier schema
  2. read gold/leads_scored -> seed new sirens as `pending` (priority = score)
  3. for the highest-priority `pending`, DuckDuckGo search -> domain
     -> status `resolved_unverified` (verification happens in the crawl worker)
     or `no_domain` (retried next run)

Polite: sequential, ~1 request / --delay seconds against DDG HTML.
"""

from __future__ import annotations

import argparse
import time

import pandas as pd

from ddgs import DDGS

from app.config.settings import settings
from ingestion import _crawl_db as db
from ingestion._discovery import DiscoveryError, discover_domain


def _gold_rows() -> list[dict]:
    b = settings.s3_bucket
    df = pd.read_parquet(
        f"s3://{b}/{settings.gold_leads_path}",
        columns=["siren", "raison_sociale", "commune", "score", "run_date"],
        storage_options=settings.storage_options,
    )
    if "run_date" in df.columns and not df.empty:
        rd = df["run_date"].astype(str)
        df = df[rd == rd.max()]
    df = df.dropna(subset=["siren"]).drop_duplicates("siren")
    return [
        {
            "siren": str(r.siren),
            "priority": int(r.score) if pd.notna(r.score) else 0,
            "raison_sociale": None if pd.isna(r.raison_sociale) else str(r.raison_sociale),
            "commune": None if pd.isna(r.commune) else str(r.commune),
        }
        for r in df.itertuples(index=False)
    ]


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--limit", type=int, default=None, help="max sirens to resolve this run")
    ap.add_argument("--delay", type=float, default=2.0, help="seconds between DDG requests")
    ap.add_argument("--seed-only", action="store_true", help="refresh the frontier from Gold, no discovery")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not settings.is_s3:
        raise SystemExit("LAKE_ROOT must be an s3:// URI")

    db.ensure_schema()

    rows = _gold_rows()
    seeded = db.seed_pending(rows)
    print(f"frontier seeded/refreshed from Gold: {len(rows):,} sirens ({seeded} upserts)")
    if args.seed_only:
        return

    todo = db.pending_for_discovery(limit=args.limit)
    print(f"{len(todo):,} sirens pending discovery (delay={args.delay}s)")
    if args.dry_run or not todo:
        return

    ddgs = DDGS()
    ok = miss = 0
    consec_err = 0
    t0 = time.time()
    for i, row in enumerate(todo):
        try:
            dom = discover_domain(row["raison_sociale"], row["commune"], ddgs)
            consec_err = 0
        except DiscoveryError as e:
            consec_err += 1
            print(f"  ! {row['siren']}: {e}")
            if consec_err >= 10:
                raise SystemExit(
                    f"aborted: {consec_err} consecutive DDG failures "
                    f"(rate limit / IP block). {ok} resolved so far."
                )
            continue  # leave this siren `pending`, retry next run
        db.set_discovery_result(row["siren"], dom)
        ok += bool(dom)
        miss += not dom
        if i % 200 == 0:
            rate = (i + 1) / max(time.time() - t0, 1e-6)
            print(f"  {i}/{len(todo)}  resolved={ok} no_domain={miss}  ({rate:.1f}/s)", flush=True)
        time.sleep(args.delay)

    print(f"done: {ok:,} resolved, {miss:,} no_domain, in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
