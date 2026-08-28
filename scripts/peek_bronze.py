"""Look at a Bronze SIRENE file without pulling the whole thing.

    python scripts/peek_bronze.py --dataset stock_unite_legale --rows 12
    python scripts/peek_bronze.py --dataset stock_etablissement --rows 12 --dict

Reads only the first row-groups of the Parquet in the latest partition (a few MB
of network). --dict also prints the official INSEE variable dictionary.
"""

from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path

import httpx
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config.settings import settings  # noqa: E402
from ingestion._s3 import get_fs  # noqa: E402

SLUG = {
    "stock_unite_legale": "stockunitelegale",
    "stock_etablissement": "stocketablissement",
}


def latest_partition(fs, dataset: str) -> str:
    base = f"{settings.s3_bucket}/{settings.bronze_prefix}/source=sirene/dataset={dataset}"
    parts = [p for p in fs.ls(base) if "data_version=" in p]
    if not parts:
        raise SystemExit(f"No Bronze partition under s3://{base}")
    return sorted(parts)[-1]


def print_dictionary(dataset: str) -> None:
    needle = f"{SLUG[dataset]}-311-dessin-de-fichier.csv"
    resp = httpx.get(settings.sirene_dataset_api_url, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    url = next(
        (r["url"] for r in resp.json().get("resources", []) if (r.get("url") or "").endswith(needle)),
        None,
    )
    if not url:
        print("(variable dictionary not found on data.gouv)")
        return
    doc = httpx.get(url, timeout=60, follow_redirects=True)
    doc.raise_for_status()
    dico = pd.read_csv(io.BytesIO(doc.content), dtype=str, sep=";")
    print("\n=== Dictionnaire officiel des variables (INSEE) ===")
    with pd.option_context("display.max_rows", None, "display.max_colwidth", 70, "display.width", 200):
        print(dico.to_string(index=False))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", choices=sorted(SLUG), default="stock_unite_legale")
    ap.add_argument("--rows", type=int, default=12)
    ap.add_argument("--dict", action="store_true", help="also print the INSEE variable dictionary")
    args = ap.parse_args()

    fs = get_fs()
    part = latest_partition(fs, args.dataset)
    pq_files = [f for f in fs.ls(part) if f.endswith(".parquet")]
    if not pq_files:
        raise SystemExit(f"No .parquet in s3://{part} (re-ingest with --format parquet)")
    path = pq_files[0]
    print(f"Reading: s3://{path}")

    with fs.open(path, "rb") as fh:
        pf = pq.ParquetFile(fh)
        print(f"Total rows: {pf.metadata.num_rows:,}   Row groups: {pf.num_row_groups}")
        df = pf.read_row_group(0).to_pandas().head(args.rows)

    print(f"\n=== Colonnes ({len(df.columns)}) ===")
    for c in df.columns:
        print(f"  - {c}")

    print(f"\n=== {len(df)} premières lignes ===")
    with pd.option_context("display.max_columns", None, "display.max_colwidth", 28, "display.width", 240):
        print(df.to_string(index=False))

    if args.dict:
        print_dictionary(args.dataset)


if __name__ == "__main__":
    main()
