"""Delete objects in bronze/source=sirene partitions that are not the kept format.

Keeps: *.<--keep>, manifest.json, _SUCCESS. Lists everything else; deletes with --apply.

    python scripts/clean_bronze.py --keep parquet           # dry-run
    python scripts/clean_bronze.py --keep parquet --apply
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config.settings import settings  # noqa: E402
from ingestion._s3 import get_fs  # noqa: E402

ALWAYS_KEEP = {"manifest.json", "_SUCCESS"}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--keep", default="parquet", help="file extension to keep (default: parquet)")
    ap.add_argument("--apply", action="store_true", help="actually delete (default: dry-run)")
    args = ap.parse_args()

    fs = get_fs()
    root = f"{settings.s3_bucket}/{settings.bronze_prefix}/source=sirene"
    victims: list[str] = []

    for obj in fs.find(root):
        name = obj.rsplit("/", 1)[-1]
        if name in ALWAYS_KEEP or name.endswith(f".{args.keep}"):
            continue
        victims.append(obj)

    if not victims:
        print("Nothing to delete.")
        return

    print(f"{'DELETING' if args.apply else 'Would delete'} {len(victims)} object(s):")
    for v in victims:
        print(f"  s3://{v}")

    if args.apply:
        fs.rm(victims)
        print("Done.")
    else:
        print("\n(dry-run) re-run with --apply to delete.")


if __name__ == "__main__":
    main()
