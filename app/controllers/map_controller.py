"""Map layers served from Gold: points (leads_scored) + zone aggregates."""

import time

import numpy as np
import pandas as pd

from app.config.settings import settings
from app.controllers.leads_controller import _clean, _latest_run
from app.services.lake_service import read_dataset

# The map hits Gold on every filter change / pan; re-reading the whole
# leads_scored Parquet from object storage each time is the main latency.
# Keep the frames in-process for a few minutes.
_TTL_SECONDS = 300
_cache: dict[str, tuple[float, pd.DataFrame]] = {}


def _read_cached(*parts: str) -> pd.DataFrame:
    key = "/".join(parts)
    hit = _cache.get(key)
    if hit is not None and (time.time() - hit[0]) < _TTL_SECONDS:
        return hit[1]
    df = read_dataset(*parts)
    _cache[key] = (time.time(), df)
    return df


# --- vector tiles: serve /map/tiles/{z}/{x}/{y}.mvt out of gold/tiles/leads.pmtiles
# On load we walk the whole pmtiles directory tree once into {tile_id: (off,len)}
# so every subsequent tile is a dict lookup + byte slice (no per-request parsing).
_TILES_PATH = "gold/tiles/leads.pmtiles"
_pm: dict = {"t": 0.0, "data": b"", "index": None, "enc": None, "minz": 0, "maxz": 14}


def _load_pmtiles() -> dict:
    try:
        from pmtiles.reader import Reader
        from pmtiles.tile import Compression, deserialize_directory

        from ingestion._s3 import get_fs

        fs = get_fs()
        path = f"{settings.s3_bucket}/{_TILES_PATH}"
        if not fs.exists(path):
            _pm.update(t=time.time(), index=None)
            return _pm

        with fs.open(path, "rb") as fh:
            data = fh.read()
        hdr = Reader(lambda off, length: data[off : off + length]).header()

        tdo = hdr["tile_data_offset"]
        ldo = hdr["leaf_directory_offset"]
        index: dict[int, tuple[int, int]] = {}

        def walk(dir_off: int, dir_len: int) -> None:
            for e in deserialize_directory(data[dir_off : dir_off + dir_len]):
                if e.run_length == 0:  # pointer to a leaf directory
                    walk(ldo + e.offset, e.length)
                else:
                    for k in range(e.run_length):
                        index[e.tile_id + k] = (tdo + e.offset, e.length)

        walk(hdr["root_offset"], hdr["root_length"])
        _pm.update(
            t=time.time(),
            data=data,
            index=index,
            enc="gzip" if hdr.get("tile_compression") == Compression.GZIP else None,
            minz=hdr.get("min_zoom", 0),
            maxz=hdr.get("max_zoom", 14),
        )
    except Exception as exc:  # noqa: BLE001 - a missing/broken archive must not 500
        import logging

        logging.getLogger(__name__).warning("pmtiles unavailable: %s", exc)
        _pm.update(t=time.time(), index=None)
    return _pm


def vector_tile(z: int, x: int, y: int) -> tuple[bytes, str | None] | None:
    """Raw MVT tile from the pmtiles archive, or None if absent / out of range."""
    pm = _pm
    if pm["index"] is None or (time.time() - pm["t"]) >= _TTL_SECONDS:
        pm = _load_pmtiles()
    if pm["index"] is None or z < pm["minz"] or z > pm["maxz"]:
        return None
    try:
        from pmtiles.tile import zxy_to_tileid

        loc = pm["index"].get(zxy_to_tileid(z, x, y))
    except Exception:  # noqa: BLE001
        return None
    if loc is None:
        return None
    off, length = loc
    return pm["data"][off : off + length], pm["enc"]

# Kept deliberately light: the map can hold ~250k points, so the bulk payload
# ships only what a marker/cluster needs. Full detail (motifs, offer link, forme
# juridique…) is fetched per lead from GET /leads/{siren} on click.
POINT_COLS = [
    "siren", "raison_sociale", "segment", "code_ape", "commune", "departement",
    "latitude", "longitude",
    "score", "bande_score", "nb_offres_90j", "nb_etablissements",
    "a_offre_paie", "a_offre_comptabilite", "a_offre_juridique",
    "a_offre_patrimoine", "a_offre_immobilier",
]

METIERS = ("paie", "comptabilite", "juridique", "patrimoine", "immobilier")


def _apply_lead_filters(
    df: pd.DataFrame,
    *,
    segment: str | None = None,
    code_ape: str | None = None,
    poste: str | None = None,
    bande: str | None = None,
    reseau: str | None = None,
    region: str | None = None,
    score_min: float | None = None,
    flag: str | None = None,
    q: str | None = None,
) -> pd.DataFrame:
    """Shared attribute filter chain for the map endpoints (bbox handled apart)."""
    if q and q.strip() and "raison_sociale" in df.columns:
        df = df[df["raison_sociale"].fillna("").str.contains(q.strip(), case=False, regex=False)]
    if region and region in REGION_DEPTS and "departement" in df.columns:
        df = df[df["departement"].astype(str).isin(REGION_DEPTS[region])]
    if segment:
        segs = [s.strip() for s in segment.split(",") if s.strip()]
        if segs:
            df = df[df["segment"].isin(segs)]
    if code_ape:
        codes = [c.strip() for c in code_ape.split(",") if c.strip()]
        if codes:
            df = df[df["code_ape"].isin(codes)]
    if poste == "any":
        if "nb_offres_90j" in df.columns:
            df = df[df["nb_offres_90j"].fillna(0) > 0]
    elif poste in METIERS:
        col = f"a_offre_{poste}"
        if col in df.columns:
            df = df[df[col].fillna(False).astype(bool)]
    if bande:
        df = df[df["bande_score"] == bande]
    if reseau in ("mono", "multi") and "nb_etablissements" in df.columns:
        n = df["nb_etablissements"].fillna(1)
        df = df[n > 1] if reseau == "multi" else df[n <= 1]
    if score_min is not None:
        df = df[df["score"] >= score_min]
    if flag and "flags" in df.columns:
        wanted = {
            _FLAG_KEY[k][2]
            for k in (x.strip() for x in flag.split(","))
            if k in _FLAG_KEY
        }
        if wanted:
            df = df[
                df["flags"].apply(
                    lambda v: bool(wanted & set(v))
                    if isinstance(v, (list, np.ndarray))
                    else False
                )
            ]
    return df

# INSEE region code -> its département codes (as they appear in `departement`).
REGION_DEPTS: dict[str, list[str]] = {
    "11": ["75", "77", "78", "91", "92", "93", "94", "95"],           # Île-de-France
    "24": ["18", "28", "36", "37", "41", "45"],                       # Centre-Val de Loire
    "27": ["21", "25", "39", "58", "70", "71", "89", "90"],           # Bourgogne-Franche-Comté
    "28": ["14", "27", "50", "61", "76"],                             # Normandie
    "32": ["02", "59", "60", "62", "80"],                             # Hauts-de-France
    "44": ["08", "10", "51", "52", "54", "55", "57", "67", "68", "88"],  # Grand Est
    "52": ["44", "49", "53", "72", "85"],                             # Pays de la Loire
    "53": ["22", "29", "35", "56"],                                   # Bretagne
    "75": ["16", "17", "19", "23", "24", "33", "40", "47", "64", "79", "86", "87"],  # Nouvelle-Aquitaine
    "76": ["09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"],  # Occitanie
    "84": ["01", "03", "07", "15", "26", "38", "42", "43", "63", "69", "73", "74"],  # Auvergne-Rhône-Alpes
    "93": ["04", "05", "06", "13", "83", "84"],                       # Provence-Alpes-Côte d'Azur
    "94": ["2A", "2B"],                                               # Corse
    "01": ["971"],  # Guadeloupe
    "02": ["972"],  # Martinique
    "03": ["973"],  # Guyane
    "04": ["974"],  # La Réunion
    "06": ["976"],  # Mayotte
}

FIRM_COLS = [
    "siret", "est_siege", "raison_sociale", "adresse", "code_postal",
    "commune", "departement", "latitude", "longitude",
]


BULK_CAP = 400_000


def map_points(
    *,
    bbox: str | None = None,
    segment: str | None = None,
    code_ape: str | None = None,
    poste: str | None = None,
    bande: str | None = None,
    reseau: str | None = None,
    region: str | None = None,
    score_min: float | None = None,
    flag: str | None = None,
    q: str | None = None,
    limit: int = 5000,
    bulk: bool = False,
    count_only: bool = False,
    breakdown_only: bool = False,
) -> dict:
    """Scored leads with coordinates: GeoJSON, or a lean columnar payload (bulk).

    bulk=True returns {lon[], lat[], siren[]} for the *whole* filtered set so the
    client can cluster it; per-lead detail is fetched from /leads/{siren}.
    """
    empty = (
        {"available": False, "format": "bulk", "count": 0, "lon": [], "lat": [], "siren": []}
        if bulk
        else {"type": "FeatureCollection", "features": [], "available": False, "count": 0}
    )
    # count / breakdown run on the slim numpy frame — no 40-column reindexing
    if count_only or breakdown_only:
        c = _cluster_frame()
        if c is None:
            return {"count": 0} if count_only else {"count": 0, "bande": {}, "segment": {}, "departement": {}}
        m = _mask(
            c, bbox=bbox, segment=segment, code_ape=code_ape, poste=poste,
            bande=bande, reseau=reseau, region=region, score_min=score_min, flag=flag, q=q,
        )
        if count_only:
            return {"count": int(m.sum())}

        def top_counts(key: str, top: int | None = None) -> dict:
            vals, cnts = np.unique(c[key][m], return_counts=True)
            order = np.argsort(cnts)[::-1]
            if top:
                order = order[:top]
            return {str(vals[i]): int(cnts[i]) for i in order}

        return {
            "count": int(m.sum()),
            "bande": top_counts("b"),
            "segment": top_counts("sg"),
            "departement": top_counts("dp", top=12),
        }

    df = _read_cached(settings.gold_leads_path)
    if df.empty:
        return empty

    df = _latest_run(df)
    df = df[df["latitude"].notna() & df["longitude"].notna()]
    df = _apply_lead_filters(
        df,
        segment=segment,
        code_ape=code_ape,
        poste=poste,
        bande=bande,
        reseau=reseau,
        region=region,
        score_min=score_min,
        flag=flag,
        q=q,
    )
    if bbox:
        try:
            w, s, e, n = (float(x) for x in bbox.split(","))
            df = df[
                (df["longitude"] >= w) & (df["longitude"] <= e)
                & (df["latitude"] >= s) & (df["latitude"] <= n)
            ]
        except ValueError:
            pass

    if "score" in df.columns:
        df = df.sort_values("score", ascending=False)

    if bulk:
        df = df.head(BULK_CAP)
        return {
            "available": True,
            "format": "bulk",
            "count": int(len(df)),
            "lon": df["longitude"].round(5).tolist(),
            "lat": df["latitude"].round(5).tolist(),
            "siren": df["siren"].astype(str).tolist(),
        }

    df = df.head(limit)

    cols = [c for c in POINT_COLS if c in df.columns]
    features = []
    for rec in df[cols].to_dict(orient="records"):
        rec = _clean(rec)
        lon = rec.pop("longitude", None)
        lat = rec.pop("latitude", None)
        # drop null/false keys — for the ~99% of leads with no signal this
        # collapses each feature to a handful of fields
        props = {k: v for k, v in rec.items() if v is not None and v is not False}
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props,
            }
        )
    return {"type": "FeatureCollection", "features": features, "available": True, "count": len(features)}


def _latest_dv(df: pd.DataFrame) -> pd.DataFrame:
    if not df.empty and "data_version" in df.columns:
        dv = df["data_version"].astype(str)
        return df[dv == dv.max()]
    return df


def map_zones(*, level: str = "departement", segment: str | None = None) -> dict:
    name = "cabinet_par_commune" if level == "commune" else "cabinet_par_departement"
    df = _read_cached(settings.gold_kpi_prefix, name)
    if df.empty:
        return {"available": False, "level": level, "rows": []}
    df = _latest_dv(df)
    if segment:
        df = df[df["segment"] == segment]
    return {"available": True, "level": level, "rows": [_clean(r) for r in df.to_dict(orient="records")]}


def _abbrev(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M".replace(".0M", "M")
    if n >= 1_000:
        return f"{n / 1_000:.1f}k".replace(".0k", "k")
    return str(n)


# Slim, latest-run, coords-clean columns as numpy arrays — masking here is ~1 ms
# per predicate vs ~30 ms reindexing the 40-column Gold frame each time.
_METIER_KEY = {
    "paie": "pp", "comptabilite": "pc", "juridique": "pj",
    "patrimoine": "pt", "immobilier": "pi",
}
# ?flag=<a,b,..> value -> (Gold column, cluster-frame key, human label)
_FLAG_KEY = {
    "ca_hausse": ("flag_ca_hausse", "fh", "CA en hausse"),
    "ca_baisse": ("flag_ca_baisse", "fb", "CA en baisse"),
    "rachat": ("flag_acquisition", "fx", "a racheté un cabinet"),
    "redressement": ("flag_redressement", "fr", "en redressement"),
    "dirigeant": ("flag_nouveau_dirigeant", "fd", "nouveau dirigeant"),
}
_cf: dict = {"t": 0.0, "cols": None}


def _cluster_frame() -> dict | None:
    if _cf["cols"] is not None and (time.time() - _cf["t"]) < _TTL_SECONDS:
        return _cf["cols"]
    df = _read_cached(settings.gold_leads_path)
    if df.empty:
        _cf.update(t=time.time(), cols=None)
        return None
    df = _latest_run(df)
    df = df[df["latitude"].notna() & df["longitude"].notna()]
    idx = df.index

    def s(col: str, default) -> pd.Series:
        return df[col] if col in df.columns else pd.Series(default, index=idx)

    cols = {
        "lon": df["longitude"].to_numpy("float64"),
        "lat": df["latitude"].to_numpy("float64"),
        "siren": s("siren", "").astype(str).to_numpy(),
        "rs": s("raison_sociale", "").astype(str).str.lower().to_numpy(),
        "b": s("bande_score", "").astype(str).to_numpy(),
        "sg": s("segment", "").astype(str).to_numpy(),
        "ap": s("code_ape", "").astype(str).to_numpy(),
        "dp": s("departement", "").astype(str).to_numpy(),
        "of": pd.to_numeric(s("nb_offres_90j", 0), errors="coerce").fillna(0).to_numpy("int64"),
        "ne": pd.to_numeric(s("nb_etablissements", 1), errors="coerce").fillna(1).to_numpy("int64"),
        "sc": pd.to_numeric(s("score", 0), errors="coerce").fillna(0).to_numpy("float64"),
    }
    for metier, key in _METIER_KEY.items():
        cols[key] = s(f"a_offre_{metier}", False).fillna(False).astype(bool).to_numpy()
    for gcol, key, _label in _FLAG_KEY.values():
        cols[key] = s(gcol, False).fillna(False).astype(bool).to_numpy()
    _cf.update(t=time.time(), cols=cols)
    return cols


def _mask(
    c: dict,
    *,
    bbox: str | None = None,
    segment: str | None = None,
    code_ape: str | None = None,
    poste: str | None = None,
    bande: str | None = None,
    reseau: str | None = None,
    region: str | None = None,
    score_min: float | None = None,
    flag: str | None = None,
    q: str | None = None,
) -> np.ndarray:
    m = np.ones(len(c["lon"]), dtype=bool)
    if bbox:
        try:
            w, s_, e, n_ = (float(x) for x in bbox.split(","))
            m &= (c["lon"] >= w) & (c["lon"] <= e) & (c["lat"] >= s_) & (c["lat"] <= n_)
        except (ValueError, AttributeError):
            return np.zeros(len(c["lon"]), dtype=bool)
    if region and region in REGION_DEPTS:
        m &= np.isin(c["dp"], REGION_DEPTS[region])
    if segment:
        segs = [x.strip() for x in segment.split(",") if x.strip()]
        if segs:
            m &= np.isin(c["sg"], segs)
    if code_ape:
        codes = [x.strip() for x in code_ape.split(",") if x.strip()]
        if codes:
            m &= np.isin(c["ap"], codes)
    if poste == "any":
        m &= c["of"] > 0
    elif poste in _METIER_KEY:
        m &= c[_METIER_KEY[poste]]
    if bande:
        m &= c["b"] == bande
    if reseau == "multi":
        m &= c["ne"] > 1
    elif reseau == "mono":
        m &= c["ne"] <= 1
    if score_min is not None:
        m &= c["sc"] >= score_min
    if flag:
        keys = [x.strip() for x in flag.split(",") if x.strip() in _FLAG_KEY]
        if keys:
            fm = np.zeros(len(c["lon"]), dtype=bool)
            for k in keys:
                fm |= c[_FLAG_KEY[k][1]]
            m &= fm
    if q and q.strip():
        m &= np.char.find(c["rs"].astype(str), q.strip().lower()) >= 0
    return m


def map_clusters(
    *,
    bbox: str,
    zoom: float,
    segment: str | None = None,
    code_ape: str | None = None,
    poste: str | None = None,
    bande: str | None = None,
    reseau: str | None = None,
    region: str | None = None,
    score_min: float | None = None,
    flag: str | None = None,
    q: str | None = None,
) -> dict:
    """Grid-aggregated leads for the current viewport: numbered clusters where a
    cell holds >1 lead, individual points (with siren) where it holds exactly 1.
    Counts are exact for any filter combination."""
    c = _cluster_frame()
    if c is None:
        return {"clusters": [], "points": [], "count": 0}

    m = _mask(
        c,
        bbox=bbox,
        segment=segment,
        code_ape=code_ape,
        poste=poste,
        bande=bande,
        reseau=reseau,
        region=region,
        score_min=score_min,
        flag=flag,
        q=q,
    )
    total = int(m.sum())
    if total == 0:
        return {"clusters": [], "points": [], "count": 0}

    lon, lat = c["lon"][m], c["lat"][m]
    z = max(0.0, min(22.0, float(zoom)))
    p = 360.0 / (2.0 ** (z + 3.0))  # grid cell size in degrees (~50 px)
    work = pd.DataFrame(
        {
            "lon": lon,
            "lat": lat,
            "siren": c["siren"][m],
            "b": c["b"][m],
            "cx": np.floor(lon / p).astype("int64"),
            "cy": np.floor(lat / p).astype("int64"),
        }
    )
    agg = work.groupby(["cx", "cy"], sort=False).agg(
        n=("lon", "size"),
        lon=("lon", "mean"),
        lat=("lat", "mean"),
        siren=("siren", "first"),
        b=("b", "first"),
    )
    clusters = [
        {"lon": round(r.lon, 5), "lat": round(r.lat, 5), "count": int(r.n), "label": _abbrev(int(r.n))}
        for r in agg[agg["n"] > 1].itertuples()
    ]
    points = [
        {"lon": round(r.lon, 5), "lat": round(r.lat, 5), "siren": r.siren, "b": r.b}
        for r in agg[agg["n"] == 1].itertuples()
    ]
    return {"clusters": clusters, "points": points, "count": total}


def firm_network(siren: str) -> dict:
    """Every establishment of one firm (siège + secondaires) from silver/cabinet,
    for the click-to-link map view."""
    df = _read_cached("silver", "cabinet")
    if df.empty:
        return {"available": False, "siren": siren, "count": 0, "etablissements": []}
    df = _latest_dv(df)
    df = df[df["siren"].astype(str) == str(siren)]
    if df.empty:
        return {"available": True, "siren": siren, "count": 0, "etablissements": []}
    if "est_siege" in df.columns:
        df = df.sort_values("est_siege", ascending=False)
    cols = [c for c in FIRM_COLS if c in df.columns]
    recs = [_clean(r) for r in df[cols].to_dict(orient="records")]
    raison = next((r.get("raison_sociale") for r in recs if r.get("raison_sociale")), None)
    return {
        "available": True,
        "siren": str(siren),
        "raison_sociale": raison,
        "count": len(recs),
        "etablissements": recs,
    }


def signaux_du_jour(*, limit: int = 200) -> dict:
    df = _read_cached(settings.gold_kpi_prefix, "signaux_du_jour")
    if df.empty:
        return {"available": False, "items": []}
    df = _latest_dv(df)
    if "score" in df.columns:
        df = df.sort_values("score", ascending=False)
    df = df.head(limit)
    return {"available": True, "items": [_clean(r) for r in df.to_dict(orient="records")]}
