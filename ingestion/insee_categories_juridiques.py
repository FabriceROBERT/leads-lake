"""Ingest the INSEE 'catégories juridiques' nomenclature (niveau III).

    python -m ingestion.insee_categories_juridiques [--force]

- raw .xls  -> bronze/source=insee_ref/dataset=categories_juridiques/data_version=<date>/
- parsed    -> silver/_reference/categories_juridiques/   (code, libelle)  -- a conformed
              dimension, not versioned; overwritten each run.
"""

from __future__ import annotations

import argparse
import io
import json
import re
from datetime import datetime, timezone

import httpx
import pandas as pd

from app.config.settings import settings
from ingestion._s3 import get_fs

FR_MONTHS = {
    "janvier": 1, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5, "juin": 6,
    "juillet": 7, "aout": 8, "septembre": 9, "octobre": 10, "novembre": 11, "decembre": 12,
}


def _version_from_name(name: str) -> str:
    slug = name.lower().translate(str.maketrans("éèêûùç", "eeeuuc"))
    m = re.search(r"cj_([a-z]+)_(\d{4})", slug)
    if m and m.group(1) in FR_MONTHS:
        return f"{m.group(2)}-{FR_MONTHS[m.group(1)]:02d}-01"
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--force", action="store_true", help="re-ingest even if already present")
    args = ap.parse_args()

    if not settings.is_s3:
        raise SystemExit(f"LAKE_ROOT must be s3://<bucket> (got {settings.lake_root!r})")

    fs = get_fs()
    bucket = settings.s3_bucket
    url = settings.insee_cj_url
    fname = url.rsplit("/", 1)[-1]
    dv = _version_from_name(fname)

    prefix = f"{settings.bronze_prefix}/source=insee_ref/dataset=categories_juridiques/data_version={dv}/"
    success = f"{bucket}/{prefix}_SUCCESS"
    ref_key = f"{bucket}/silver/_reference/categories_juridiques/part-0.parquet"

    if fs.exists(success) and fs.exists(ref_key) and not args.force:
        print(f"Already ingested: s3://{success}  (use --force to redo)")
        return

    print(f"Downloading {url}")
    content = httpx.get(url, timeout=60, follow_redirects=True).content

    # 1. raw copy -> Bronze (provenance)
    fs.pipe(f"{bucket}/{prefix}{fname}", content)
    manifest = {
        "source": "insee_ref",
        "dataset": "categories_juridiques",
        "data_version": dv,
        "source_url": url,
        "filename": fname,
        "bytes": len(content),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    fs.pipe(f"{bucket}/{prefix}manifest.json", json.dumps(manifest, indent=2).encode())
    fs.pipe(success, b"")
    print(f"  raw  -> s3://{bucket}/{prefix}{fname}")

    # 2. parsed niveau III -> silver/_reference
    xls = pd.ExcelFile(io.BytesIO(content), engine="xlrd")
    df = xls.parse("Niveau III", header=None, dtype=str, skiprows=4).iloc[:, :2]
    df.columns = ["code", "libelle"]
    df = df[df["code"].str.fullmatch(r"\d{4}", na=False)].copy()
    df["libelle"] = df["libelle"].str.strip()
    df = df.reset_index(drop=True)

    df.to_parquet(f"s3://{ref_key}", index=False, storage_options=settings.storage_options)
    print(f"  ref  -> s3://{ref_key}  ({len(df)} codes)")
    print(df.head(6).to_string(index=False))


if __name__ == "__main__":
    main()
