"""Contact-crawl, step 2: fetch resolved domains -> phone / e-mail / SIREN check.

    python -m ingestion.crawl_worker --limit 1000 --delay 1.5

Reads `crawl_frontier` rows that are due (`next_due_at <= now()`), fetches the
homepage + a few contact/legal pages of each domain, extracts telephones /
e-mails, and verifies the target SIREN appears on the site
(`/mentions-legales`). Writes one Bronze row per firm and reschedules
`next_due_at` by band.

Bronze: bronze/source=crawl/dataset=contacts/data_version=<YYYY-MM-DD>/part-<run>.parquet
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import time
import urllib.parse
import urllib.robotparser
import uuid

import httpx
import pandas as pd

from app.config.settings import settings
from ingestion import _crawl_db as db
from ingestion._crawl_extract import contact_links, emails, phones, sirens
from ingestion._s3 import get_fs

UA = (
    "PapperlessLeadsBot/1.0 (+https://papperless-leads.duckdns.org/bot; "
    "contact enrichment for B2B prospecting)"
)
SEED_PATHS = ["/", "/mentions-legales", "/mentions-legales/", "/contact", "/contact/"]
MAX_PAGES = 8


def _band_days(priority: int, status: str) -> int:
    if status == "dead":
        return 365
    if status == "crawl_failed":
        return 14
    if priority >= 70:
        return 30
    if priority >= 45:
        return 90
    return 180


def _robots_ok(client: httpx.Client, base: str) -> bool:
    rp = urllib.robotparser.RobotFileParser()
    try:
        r = client.get(f"{base}/robots.txt", timeout=8)
        if r.status_code >= 400:
            return True  # no robots => allowed
        rp.parse(r.text.splitlines())
        return rp.can_fetch(UA, base + "/")
    except Exception:  # noqa: BLE001 - bad host / robots -> treat as allowed
        return True


def _crawl_one(client: httpx.Client, domaine: str, target_siren: str) -> dict:
    base = f"https://{domaine}"
    tel: list[str] = []
    mail: list[str] = []
    found_sirens: set[str] = set()
    pages = 0
    ok = False
    todo = list(SEED_PATHS)
    seen: set[str] = set()

    while todo and pages < MAX_PAGES:
        path = todo.pop(0)
        url = path if path.startswith("http") else base + path
        if url in seen:
            continue
        seen.add(url)
        try:
            r = client.get(url)
        except Exception:  # noqa: BLE001 - malformed URL / TLS / redirect loop -> skip page
            continue
        pages += 1
        if r.status_code >= 400 or "text/html" not in r.headers.get("content-type", ""):
            continue
        ok = True
        html = r.text
        for p in phones(html):
            if p not in tel:
                tel.append(p)
        for e in emails(html):
            if e not in mail:
                mail.append(e)
        found_sirens |= sirens(html)
        for href in contact_links(html, domaine):
            nxt = urllib.parse.urljoin(url, href)
            if nxt.startswith(base) and nxt not in seen:
                todo.append(nxt)

    verified = target_siren[:9] in found_sirens
    return {
        "siren": target_siren,
        "domaine": domaine,
        "url_source": base,
        "telephones": tel,
        "emails": mail,
        "siren_verifie_sur_site": verified,
        "http_ok": ok,
        "pages_vues": pages,
        "crawled_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def _write_bronze(rows: list[dict]) -> str:
    fs = get_fs()
    base = f"{settings.s3_bucket}/{settings.bronze_prefix}"
    data_version = dt.date.today().isoformat()
    run = uuid.uuid4().hex[:8]
    part = f"{base}/source=crawl/dataset=contacts/data_version={data_version}/part-{run}.parquet"
    df = pd.DataFrame(rows)
    for col in ("telephones", "emails"):
        df[col] = df[col].apply(lambda v: v if isinstance(v, list) else [])
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    with fs.open(part, "wb") as fh:
        fh.write(buf.read())
    with fs.open(f"{part.rsplit('/', 1)[0]}/_SUCCESS", "w") as fh:
        fh.write("")
    return part


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--limit", type=int, default=1000, help="max domains this run")
    ap.add_argument("--delay", type=float, default=1.5, help="seconds between domains")
    ap.add_argument("--flush", type=int, default=500, help="rows per Bronze part file (checkpoint)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not settings.is_s3:
        raise SystemExit("LAKE_ROOT must be an s3:// URI")

    db.ensure_schema()
    todo = db.due_for_crawl(limit=args.limit)
    print(f"{len(todo):,} domains due for crawl (delay={args.delay}s)")
    if args.dry_run or not todo:
        return

    client = httpx.Client(
        timeout=12,
        follow_redirects=True,
        headers={"User-Agent": UA, "Accept-Language": "fr-FR,fr;q=0.9"},
    )
    rows: list[dict] = []
    written = verified = with_contact = dead = 0
    t0 = time.time()

    def flush() -> None:
        nonlocal rows, written
        if rows:
            part = _write_bronze(rows)
            written += len(rows)
            print(f"  flushed {len(rows):,} -> s3://{part}", flush=True)
            rows = []

    try:
        for i, row in enumerate(todo):
            try:
                base = f"https://{row['domaine']}"
                robots = _robots_ok(client, base)
                if not robots:
                    db.set_crawl_result(row["siren"], "crawl_failed", 30)
                    continue
                rec = _crawl_one(client, row["domaine"], row["siren"])
                rec["robots_ok"] = robots
                rows.append(rec)

                has_contact = bool(rec["telephones"] or rec["emails"])
                if not rec["http_ok"]:
                    status = "dead"
                elif rec["siren_verifie_sur_site"] and has_contact:
                    status = "resolved_verified"
                else:
                    status = "resolved_unverified"
                verified += status == "resolved_verified"
                with_contact += has_contact
                dead += status == "dead"
                db.set_crawl_result(row["siren"], status, _band_days(row["priority"], status))
            except Exception as e:  # noqa: BLE001 - never let one domain kill the run
                print(f"  ! {row.get('domaine')}: {type(e).__name__} {e}", flush=True)
                db.set_crawl_result(row["siren"], "crawl_failed", 14)

            if len(rows) >= args.flush:
                flush()
            if i % 100 == 0:
                rate = (i + 1) / max(time.time() - t0, 1e-6)
                print(
                    f"  {i}/{len(todo)}  verified={verified} contact={with_contact} dead={dead}"
                    f"  ({rate:.1f}/s)",
                    flush=True,
                )
            time.sleep(args.delay)
    finally:
        client.close()
        flush()

    print(
        f"done: {written:,} rows written, {verified:,} verified, "
        f"{with_contact:,} with a phone/e-mail, {dead:,} dead, in {time.time() - t0:.0f}s"
    )


if __name__ == "__main__":
    main()
