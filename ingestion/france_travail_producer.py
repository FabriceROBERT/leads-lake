"""Poll France Travail offers for the target NAF codes -> Kafka topic.

    python -m ingestion.france_travail_producer --once
    python -m ingestion.france_travail_producer --loop --interval 600
    python -m ingestion.france_travail_producer --once --from 2026-01-01T00:00:00Z   # backfill
    python -m ingestion.france_travail_producer --dry-run                            # count, no Kafka

Watermark (per NAF, max dateCreation seen) is persisted to
s3://<bucket>/_state/france_travail_watermark.json so restarts don't re-scan.
Silver dedups on offre id anyway, so a small overlap is harmless.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

from app.config.settings import settings
from ingestion._france_travail import search_offres
from ingestion._s3 import get_fs

# hiring-company NAF = target segments (same as silver_cabinet, minus 70.22Z)
NAF_CODES = [
    "41.10A", "41.10B", "41.10C",
    "66.12Z", "66.19B",
    "69.10Z", "69.20Z",
    "74.90B", "82.11Z",
]

WATERMARK_KEY = "_state/france_travail_watermark.json"
PAGE = 150
MAX_ROWS = 3000  # FT pagination hard cap (~3149)
EPOCH = "2026-01-01T00:00:00Z"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _trunc(iso: str) -> str:
    # 2026-08-27T15:11:40.591Z -> 2026-08-27T15:11:40Z
    return (iso.split(".")[0].rstrip("Z") + "Z") if iso else iso


def load_watermark(fs, bucket: str) -> dict:
    key = f"{bucket}/{WATERMARK_KEY}"
    return json.loads(fs.cat_file(key)) if fs.exists(key) else {}


def save_watermark(fs, bucket: str, wm: dict) -> None:
    fs.pipe(f"{bucket}/{WATERMARK_KEY}", json.dumps(wm, indent=2).encode())


def poll_once(producer, fs, bucket: str, since_override: str | None) -> int:
    wm = load_watermark(fs, bucket)
    now = _now()
    sent = 0
    for naf in NAF_CODES:
        since = since_override or wm.get(naf) or EPOCH
        newest = since
        offset = 0
        while offset < MAX_ROWS:
            offres, total = search_offres(
                {
                    "codeNAF": naf,
                    "minCreationDate": since,
                    "maxCreationDate": now,
                    "sort": "1",
                    "range": f"{offset}-{offset + PAGE - 1}",
                }
            )
            if not offres:
                break
            for o in offres:
                if producer is not None:
                    producer.send(
                        settings.kafka_topic_ft,
                        key=o["id"].encode(),
                        value=json.dumps(o, ensure_ascii=False).encode(),
                    )
                dc = o.get("dateCreation", "")
                if dc > newest:
                    newest = dc
                sent += 1
            offset += PAGE
            if offset >= total:
                break
        wm[naf] = _trunc(newest) if newest != since else now
    if producer is not None:
        producer.flush()
        save_watermark(fs, bucket, wm)  # advance watermark only on a real produce
    return sent


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--once", action="store_true", help="one poll cycle then exit (default)")
    ap.add_argument("--loop", action="store_true", help="poll forever")
    ap.add_argument("--interval", type=int, default=600, help="seconds between cycles in --loop")
    ap.add_argument("--from", dest="from_", default=None, help="override watermark start (not persisted)")
    ap.add_argument("--dry-run", action="store_true", help="count offers, do not produce to Kafka")
    args = ap.parse_args()

    fs = get_fs()
    bucket = settings.s3_bucket

    if args.dry_run:
        n = poll_once(None, fs, bucket, args.from_)
        print(f"[dry-run] {n} offres would be produced to {settings.kafka_topic_ft}")
        return

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap.split(","),
        linger_ms=200,
        retries=3,
        acks="all",
    )

    try:
        if args.loop:
            while True:
                t0 = time.time()
                n = poll_once(producer, fs, bucket, args.from_)
                print(f"{_now()}  produced {n}  ({time.time() - t0:.1f}s)")
                time.sleep(max(0, args.interval - (time.time() - t0)))
        else:
            n = poll_once(producer, fs, bucket, args.from_)
            print(f"produced {n} offres -> {settings.kafka_topic_ft}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
