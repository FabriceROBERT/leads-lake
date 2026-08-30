"""Batch: financials per SIREN from the open "Ratios Financiers (BCE / INPI)"
dataset -> Bronze.  Replaces the per-SIREN recherche-entreprises calls for the
`ca` / `resultat_net` signals (that API bans bulk callers).

    python -m ingestion.ratios_financiers

Source : data.economie.gouv.fr / dataset `ratios_inpi_bce` (6.5M rows), one row
per (siren, date de clôture, type de bilan). We keep the two most recent
exercises per SIREN of the parc.

Writes  bronze/source=ratios_financiers/dataset=comptes/data_version=<YYYY-MM-DD>/
        part-000.parquet  (+ manifest.json + _SUCCESS)
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import tempfile
import time

import httpx
import pandas as pd

from app.config.settings import settings
from ingestion._s3 import get_fs

DATASET = "comptes"
# Opendatasoft export, straight from the source platform (data.gouv.fr refuses
# our server's IP). Parquet, only the columns we need.
EXPORT_URL = (
    "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/ratios_inpi_bce"
    "/exports/parquet"
    "?select=siren,date_cloture_exercice,type_bilan,chiffre_d_affaires,resultat_net"
)
_BILAN_RANK = {"C": 0, "K": 1, "S": 2}  # prefer complete > consolidated > simplified


def _download(url: str) -> str:
    fd = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    n = mark = 0
    timeout = httpx.Timeout(connect=30.0, read=300.0, write=30.0, pool=30.0)
    print("downloading (Opendatasoft may take ~1 min to start)…", flush=True)
    with httpx.stream("GET", url, follow_redirects=True, timeout=timeout) as r:
        r.raise_for_status()
        for chunk in r.iter_bytes(1 << 20):
            fd.write(chunk)
            n += len(chunk)
            if n - mark >= 25 * (1 << 20):
                mark = n
                print(f"  … {n / 1e6:.0f} MB", flush=True)
    fd.close()
    print(f"downloaded {n / 1e6:.0f} MB -> {fd.name}", flush=True)
    return fd.name


def _parc_sirens() -> set[str]:
    df = pd.read_parquet(
        f"s3://{settings.s3_bucket}/silver/cabinet",
        columns=["siren", "data_version"],
        storage_options=settings.storage_options,
    )
    dv = df["data_version"].astype(str)
    return {str(s) for s in df.loc[dv == dv.max(), "siren"]}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--all", action="store_true", help="keep every SIREN, not just the parc")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not settings.is_s3:
        raise SystemExit("LAKE_ROOT must be an s3:// URI")

    t0 = time.time()
    path = _download(EXPORT_URL)
    df = pd.read_parquet(path)
    print(f"{len(df):,} rows read  (columns: {list(df.columns)})", flush=True)

    df = df.dropna(subset=["siren", "date_cloture_exercice"])
    df["siren"] = df["siren"].astype("string").str.zfill(9)
    df["annee"] = pd.to_datetime(df["date_cloture_exercice"], errors="coerce").dt.year
    df = df.dropna(subset=["annee"])
    df["annee"] = df["annee"].astype(int)
    df["ca"] = pd.to_numeric(df["chiffre_d_affaires"], errors="coerce")
    df["rn"] = pd.to_numeric(df["resultat_net"], errors="coerce")
    df["_rank"] = df["type_bilan"].map(_BILAN_RANK).fillna(3)

    # CA <= 0 = holding bilan with no revenue line, not a real zero -> ignore the row
    df = df[df["ca"] > 0]

    if not args.all:
        parc = _parc_sirens()
        df = df[df["siren"].isin(parc)]
        print(f"{len(df):,} rows after parc + CA>0 filter ({len(parc):,} sirens)", flush=True)

    # one row per (siren, année): best bilan type wins
    df = df.sort_values(["siren", "annee", "_rank"]).drop_duplicates(["siren", "annee"], keep="first")
    # two most recent exercises per siren
    df = df.sort_values(["siren", "annee"], ascending=[True, False])
    g = df.groupby("siren", sort=False)
    latest = g.nth(0)
    prev = g.nth(1)

    out = latest[["siren", "annee", "ca", "rn"]].rename(
        columns={"annee": "annee_comptes", "ca": "ca", "rn": "resultat_net"}
    )
    p = prev[["siren", "annee", "ca", "rn"]].rename(
        columns={"annee": "annee_comptes_n1", "ca": "ca_n1", "rn": "resultat_n1"}
    )
    out = out.merge(p, on="siren", how="left")
    for c in ("ca", "resultat_net", "ca_n1", "resultat_n1"):
        out[c] = out[c].round().astype("Int64")

    print(
        f"{len(out):,} sirens with financials | "
        f"CA médian {out['ca'].dropna().median():,.0f} € | "
        f"{out['ca_n1'].notna().sum():,} with N-1",
        flush=True,
    )
    if args.dry_run:
        return

    fs = get_fs()
    data_version = dt.date.today().isoformat()
    prefix = (
        f"{settings.s3_bucket}/{settings.bronze_prefix}"
        f"/source=ratios_financiers/dataset={DATASET}/data_version={data_version}"
    )
    buf = io.BytesIO()
    out.to_parquet(buf, index=False)
    buf.seek(0)
    with fs.open(f"{prefix}/part-000.parquet", "wb") as fh:
        fh.write(buf.read())
    with fs.open(f"{prefix}/manifest.json", "w") as fh:
        json.dump(
            {
                "source": "data.economie.gouv.fr/ratios_inpi_bce",
                "dataset": DATASET,
                "data_version": data_version,
                "rows": int(len(out)),
                "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            },
            fh,
            indent=2,
        )
    with fs.open(f"{prefix}/_SUCCESS", "w") as fh:
        fh.write("")
    print(f"wrote s3://{prefix}/part-000.parquet  ({len(out):,} rows, {time.time() - t0:.0f}s)")


if __name__ == "__main__":
    main()
