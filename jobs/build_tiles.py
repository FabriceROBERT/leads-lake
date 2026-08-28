"""Build `gold/tiles/leads.pmtiles` from `gold/leads_scored`.

Runs in the tiles image (docker/tiles.Dockerfile), which bundles tippecanoe:

    docker compose -f docker-compose.spark.yml run --rm tiles

Reads   s3://<bucket>/gold/leads_scored   (latest run_date)
Writes  s3://<bucket>/gold/tiles/leads.pmtiles

Each point carries the attributes the map filters on, so filtering on the
front is a pure MapLibre expression (no network): b=bande, sg=segment,
ap=code_ape, dp=departement, sc=score, ne=nb_etablissements, of=nb_offres_90j,
pp/pc/pj/pt/pi = a_offre_<metier>. si=siren (for the click → detail fetch).
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile

import pandas as pd
import s3fs


def _storage_options() -> dict:
    return {
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
        "client_kwargs": {"endpoint_url": os.environ["S3_ENDPOINT_URL"]},
        "config_kwargs": {"s3": {"addressing_style": "path"}},
    }


def main() -> None:
    bucket = os.environ["LAKE_ROOT"].split("://", 1)[-1].split("/", 1)[0]
    so = _storage_options()

    src = f"s3://{bucket}/gold/leads_scored"
    print(f"reading {src}", flush=True)
    df = pd.read_parquet(src, storage_options=so)
    if "run_date" in df.columns:
        rd = df["run_date"].astype(str)
        df = df[rd == rd.max()]
    df = df.dropna(subset=["latitude", "longitude"])
    print(f"{len(df):,} geolocated points", flush=True)

    def col(name: str, default):
        if name in df.columns:
            return df[name]
        return pd.Series([default] * len(df), index=df.index)

    si = df["siren"].astype(str).tolist()
    lon = df["longitude"].round(5).tolist()
    lat = df["latitude"].round(5).tolist()
    sc = col("score", 0).fillna(0).astype(int).tolist()
    b = col("bande_score", "froid").fillna("froid").astype(str).tolist()
    sg = col("segment", "").fillna("").astype(str).tolist()
    ap = col("code_ape", "").fillna("").astype(str).tolist()
    dp = col("departement", "").fillna("").astype(str).tolist()
    ne = col("nb_etablissements", 1).fillna(1).astype(int).tolist()
    of = col("nb_offres_90j", 0).fillna(0).astype(int).tolist()
    metier = {
        "pp": col("a_offre_paie", False).fillna(False).astype(bool).tolist(),
        "pc": col("a_offre_comptabilite", False).fillna(False).astype(bool).tolist(),
        "pj": col("a_offre_juridique", False).fillna(False).astype(bool).tolist(),
        "pt": col("a_offre_patrimoine", False).fillna(False).astype(bool).tolist(),
        "pi": col("a_offre_immobilier", False).fillna(False).astype(bool).tolist(),
    }

    ndjson = os.path.join(tempfile.gettempdir(), "leads.ndjson")
    signal_ndjson = os.path.join(tempfile.gettempdir(), "signal.ndjson")
    n_signal = 0
    with open(ndjson, "w", encoding="utf-8") as fh, open(
        signal_ndjson, "w", encoding="utf-8"
    ) as sfh:
        for i in range(len(df)):
            props = {
                "si": si[i], "sc": sc[i], "b": b[i], "sg": sg[i],
                "ap": ap[i], "dp": dp[i], "ne": ne[i], "of": of[i],
            }
            for key, vals in metier.items():
                if vals[i]:
                    props[key] = 1
            line = json.dumps(
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [lon[i], lat[i]]},
                    "properties": props,
                },
                separators=(",", ":"),
            ) + "\n"
            fh.write(line)
            if of[i] > 0:  # a cabinet with a hiring signal — never drop these
                sfh.write(line)
                n_signal += 1
    print(f"wrote {len(df):,} features ({n_signal:,} with a signal)", flush=True)

    out = os.path.join(tempfile.gettempdir(), "leads.pmtiles")
    cmd = [
        "tippecanoe",
        "-o", out,
        "-f",
        "-n", "Papperless Leads",
        "--minimum-zoom=4",
        "--maximum-zoom=13",
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        "--no-tile-size-limit",
        # layer "leads": the whole parc (decimated at low zoom)
        "-L", f"leads:{ndjson}",
        # layer "signal": only cabinets with an offer — kept at every zoom
        "-L", f"signal:{signal_ndjson}",
    ]
    print(" ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)

    dst = f"{bucket}/gold/tiles/leads.pmtiles"
    fs = s3fs.S3FileSystem(**so)
    fs.put(out, dst)
    print(f"uploaded -> s3://{dst}  ({os.path.getsize(out):,} bytes)", flush=True)


if __name__ == "__main__":
    main()
