"""Ingest one SIRENE stock file (from data.gouv) into the Bronze layer on Wasabi.

    python -m ingestion.sirene_stock --dataset stock_unite_legale --dry-run
    python -m ingestion.sirene_stock --dataset stock_unite_legale
    python -m ingestion.sirene_stock --dataset stock_etablissement --format zip

Bronze layout (raw file kept untransformed):

    bronze/source=sirene/dataset=<dataset>/data_version=<YYYYMMDD>/
        stock-<slug>-csv.zip     the file, byte-for-byte
        manifest.json            provenance (source url, size, checksum, timestamps)
        _SUCCESS                  written last -> presence == partition complete
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx

from app.config.settings import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sirene_stock")

# dataset name -> data.gouv url slug
DATASETS = {
    "stock_unite_legale": "stockunitelegale",
    "stock_etablissement": "stocketablissement",
    "stock_unite_legale_historique": "stockunitelegalehistorique",
    "stock_etablissement_historique": "stocketablissementhistorique",
    "stock_etablissement_liens_succession": "stocketablissementlienssuccession",
}

_VERSION_RE = re.compile(r"/(\d{8})-\d{6}/")
_CHUNK = 1024 * 1024


def resolve_resource(dataset: str, fmt: str) -> dict:
    """Find the current monthly resource for `dataset` in the data.gouv dataset API."""
    slug = DATASETS[dataset]
    needle = f"stock-{slug}-{'csv.zip' if fmt == 'zip' else 'parquet.parquet'}"
    resp = httpx.get(settings.sirene_dataset_api_url, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    for res in resp.json().get("resources", []):
        url = res.get("url") or ""
        if url.endswith(needle):
            match = _VERSION_RE.search(url)
            raw = match.group(1) if match else datetime.now(timezone.utc).strftime("%Y%m%d")
            # sortable ISO date: 20260801 -> 2026-08-01
            version = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
            return {
                "url": url,
                "filename": url.rsplit("/", 1)[-1],
                "filesize": res.get("filesize"),
                "checksum": res.get("checksum") or {},
                "data_version": version,
            }
    raise SystemExit(f"No resource ending with {needle!r} found in the dataset API")


def download(url: str, dest: Path, expected_size: int | None) -> None:
    if dest.exists() and expected_size and dest.stat().st_size == expected_size:
        log.info("Local file already complete: %s", dest)
        return
    part = dest.with_suffix(dest.suffix + ".part")
    done = 0
    with httpx.stream("GET", url, timeout=None, follow_redirects=True) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0)) or (expected_size or 0)
        with open(part, "wb") as fh:
            for chunk in resp.iter_bytes(chunk_size=_CHUNK):
                fh.write(chunk)
                done += len(chunk)
                if total:
                    print(
                        f"\r  {done / 1e6:,.0f} / {total / 1e6:,.0f} MB "
                        f"({done * 100 // total}%)",
                        end="",
                        flush=True,
                    )
        print()
    part.replace(dest)
    log.info("Downloaded %.0f MB -> %s", done / 1e6, dest)


def hash_file(path: Path, algo: str) -> str:
    digest = hashlib.new(algo)
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dataset", choices=sorted(DATASETS), default="stock_unite_legale")
    parser.add_argument("--format", choices=["zip", "parquet"], default="zip")
    parser.add_argument("--work-dir", default="./data")
    parser.add_argument("--dry-run", action="store_true", help="resolve + print target keys, touch nothing")
    parser.add_argument("--force", action="store_true", help="re-ingest even if _SUCCESS already exists")
    parser.add_argument("--keep-local", action="store_true", help="keep the downloaded file after upload")
    args = parser.parse_args()

    res = resolve_resource(args.dataset, args.format)
    prefix = (
        f"{settings.bronze_prefix}/source=sirene/dataset={args.dataset}"
        f"/data_version={res['data_version']}/"
    )
    log.info(
        "Resource: %s  (%s, %.0f MB, version %s)",
        res["filename"], args.format, (res["filesize"] or 0) / 1e6, res["data_version"],
    )
    log.info("Source  : %s", res["url"])
    log.info("Target  : s3://<bucket>/%s%s", prefix, res["filename"])

    if args.dry_run:
        log.info("[dry-run] stopping here.")
        return 0

    if not settings.is_s3:
        log.error("LAKE_ROOT must be s3://<bucket> for ingestion (got %r). Set it in .env", settings.lake_root)
        return 2

    from ingestion._s3 import get_fs

    bucket = settings.s3_bucket
    fs = get_fs()
    success_path = f"{bucket}/{prefix}_SUCCESS"

    if fs.exists(success_path) and not args.force:
        log.info("Already ingested: s3://%s  (use --force to redo)", success_path)
        return 0

    work = Path(args.work_dir)
    work.mkdir(parents=True, exist_ok=True)
    local = work / res["filename"]

    download(res["url"], local, res["filesize"])

    size = local.stat().st_size
    if res["filesize"] and size != res["filesize"]:
        log.error("Size mismatch: got %d expected %d", size, res["filesize"])
        return 1

    checksum = res["checksum"]
    if checksum.get("type") in ("sha1", "sha256", "md5"):
        got = hash_file(local, checksum["type"])
        if got != checksum["value"]:
            log.error("Checksum mismatch (%s): got %s expected %s", checksum["type"], got, checksum["value"])
            return 1
        log.info("Checksum OK (%s)", checksum["type"])
    else:
        checksum = {"type": "sha256", "value": hash_file(local, "sha256")}
        log.info("No upstream checksum; computed sha256=%s", checksum["value"])

    data_path = f"{bucket}/{prefix}{res['filename']}"
    log.info("Uploading -> s3://%s", data_path)
    fs.put(str(local), data_path)  # s3fs streams large files as multipart

    manifest = {
        "source": "sirene",
        "dataset": args.dataset,
        "format": args.format,
        "data_version": res["data_version"],
        "source_url": res["url"],
        "filename": res["filename"],
        "bytes": size,
        "checksum": checksum,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    fs.pipe(f"{bucket}/{prefix}manifest.json", json.dumps(manifest, indent=2).encode())
    fs.pipe(success_path, b"")  # written last: its presence means the partition is complete
    log.info("Done. Bronze partition ready: s3://%s/%s", bucket, prefix)

    if not args.keep_local:
        local.unlink(missing_ok=True)
        log.info("Removed local %s", local)
    return 0


if __name__ == "__main__":
    sys.exit(main())
