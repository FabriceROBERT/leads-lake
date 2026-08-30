"""Business logic for serving the Gold layer: leads and KPIs."""

import datetime as dt
import logging
import math
import time

import numpy as np
import pandas as pd

from app.config.settings import settings
from app.schemas.leads_schema import KpiResponse, Lead, LeadsPage
from app.services.lake_service import read_dataset

logger = logging.getLogger(__name__)

ALLOWED_KPIS = {"kpi_marche", "kpi_signaux_du_jour", "kpi_couverture"}


def _latest_run(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows from the most recent run_date partition."""
    if not df.empty and "run_date" in df.columns:
        # run_date comes from the partition path as a dictionary/categorical
        # column; cast before comparing.
        runs = df["run_date"].astype(str)
        return df[runs == runs.max()]
    return df


def _clean(rec: dict) -> dict:
    """Make a pandas record JSON-safe (NaN -> None, numpy scalars -> python)."""
    out: dict = {}
    for key, value in rec.items():
        if value is None:
            out[key] = None
        elif isinstance(value, float) and math.isnan(value):
            out[key] = None
        elif isinstance(value, np.integer):
            out[key] = int(value)
        elif isinstance(value, np.floating):
            out[key] = None if np.isnan(value) else float(value)
        elif isinstance(value, np.bool_):
            out[key] = bool(value)
        elif isinstance(value, (np.ndarray, list)):
            v = list(value)
            # pyarrow decodes a Spark map<> column (e.g. score_detail) as a list
            # of (key, value) pairs -> turn it back into a dict
            if v and all(isinstance(x, tuple) and len(x) == 2 for x in v):
                out[key] = {
                    str(k): (
                        None
                        if val is None or (isinstance(val, float) and math.isnan(val))
                        else float(val) if isinstance(val, (int, float, np.number)) else val
                    )
                    for k, val in v
                }
            else:
                out[key] = v
        elif isinstance(value, pd.Timestamp):
            out[key] = value.date().isoformat()
        elif isinstance(value, (dt.date, dt.datetime)):
            out[key] = value.isoformat()
        elif value is pd.NaT:
            out[key] = None
        else:
            out[key] = value
    return out


def list_leads(
    *,
    segment: str | None = None,
    departement: str | None = None,
    score_min: float | None = None,
    has_recent_offer: bool | None = None,
    include_clients: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> LeadsPage:
    df = read_dataset(settings.gold_leads_path)
    if df.empty:
        return LeadsPage(available=False, total=0, limit=limit, offset=offset, items=[])

    df = _latest_run(df)

    if not include_clients and "est_client" in df.columns:
        df = df[df["est_client"] != True]  # noqa: E712
    if segment and "segment" in df.columns:
        df = df[df["segment"] == segment]
    if departement and "departement" in df.columns:
        df = df[df["departement"].astype(str) == str(departement)]
    if score_min is not None and "score" in df.columns:
        df = df[df["score"] >= score_min]
    if has_recent_offer is not None and "nb_offres_30j" in df.columns:
        recent = df["nb_offres_30j"].fillna(0) > 0
        df = df[recent] if has_recent_offer else df[~recent]

    if "score" in df.columns:
        df = df.sort_values("score", ascending=False)

    total = len(df)
    page = df.iloc[offset : offset + limit]
    items = [Lead(**_clean(rec)) for rec in page.to_dict(orient="records")]
    return LeadsPage(available=True, total=total, limit=limit, offset=offset, items=items)


_enrich_cache: dict[str, tuple[float, dict | None]] = {}
_ENRICH_TTL = 86400  # 24 h — RNE/comptes change slowly


def _enrich(siren: str) -> dict:
    """Live enrichment (recherche-entreprises + BODACC), cached 24 h per SIREN."""
    hit = _enrich_cache.get(siren)
    if hit is not None and (time.time() - hit[0]) < _ENRICH_TTL:
        return hit[1] or {}
    data: dict = {}
    try:
        from ingestion._recherche_entreprises import fetch_one

        data.update(fetch_one(siren) or {})
    except Exception:  # noqa: BLE001 - enrichment must never break the lead
        logger.exception("recherche-entreprises enrichment failed for %s", siren)
    try:
        from ingestion._bodacc import fetch_siren, summarize

        data.update(summarize(fetch_siren(siren)))
    except Exception:  # noqa: BLE001
        logger.exception("BODACC enrichment failed for %s", siren)
    _enrich_cache[siren] = (time.time(), data)
    return data


def get_lead(siren: str) -> Lead | None:
    df = read_dataset(settings.gold_leads_path)
    if df.empty or "siren" not in df.columns:
        return None
    df = _latest_run(df)
    match = df[df["siren"].astype(str) == str(siren)]
    if match.empty:
        return None
    rec = _clean(match.iloc[0].to_dict())
    for key, value in _enrich(str(siren)).items():
        if value is not None and key != "siren":
            rec[key] = value
    return Lead(**rec)


def get_kpi(name: str) -> KpiResponse:
    if name not in ALLOWED_KPIS:
        raise ValueError(f"Unknown KPI '{name}'. Allowed: {sorted(ALLOWED_KPIS)}")
    df = read_dataset(settings.gold_kpi_prefix, name)
    if df.empty:
        return KpiResponse(name=name, available=False, rows=[])
    rows = [_clean(rec) for rec in df.to_dict(orient="records")]
    return KpiResponse(name=name, available=True, rows=rows)
