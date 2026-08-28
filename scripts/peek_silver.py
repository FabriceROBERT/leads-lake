"""Show a sample + distributions of silver/cabinet/ from Wasabi.

    python scripts/peek_silver.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config.settings import settings  # noqa: E402
from ingestion._s3 import get_fs  # noqa: E402


def main() -> None:
    fs = get_fs()
    base = f"{settings.s3_bucket}/silver/cabinet"
    parts = sorted(p for p in fs.ls(base) if "data_version=" in p)
    if not parts:
        raise SystemExit(f"No silver/cabinet partition under s3://{base}")
    latest = parts[-1]
    print(f"Reading: s3://{latest}")

    df = pd.read_parquet(f"s3://{latest}", storage_options=settings.storage_options)

    print(f"\nRows: {len(df):,}   Columns: {len(df.columns)}")
    print("Columns:", list(df.columns))

    print("\n=== Sample (10) ===")
    with pd.option_context("display.max_columns", None, "display.width", 240, "display.max_colwidth", 22):
        print(df.sample(min(10, len(df)), random_state=0).to_string(index=False))

    print("\n=== By segment ===")
    print(df["segment"].value_counts().to_string())

    print("\n=== Top 12 departements ===")
    print(df["departement"].value_counts().head(12).to_string())

    print("\n=== forme_juridique (top 15) ===")
    print(df["forme_juridique"].value_counts().head(15).to_string())

    print("\n=== Null rate per column ===")
    print((df.isna().mean().sort_values(ascending=False) * 100).round(1).to_string())


if __name__ == "__main__":
    main()
