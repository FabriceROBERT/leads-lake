"""Client for BODACC — bodacc-datadila.opendatasoft.com (no auth).

Annonces légales par SIREN : procédures collectives, radiations, modifications
statutaires, ventes/cessions de fonds, dépôts des comptes. Utilisé à la volée
par la couche de service et en batch par `ingestion.bodacc`.
"""

from __future__ import annotations

import datetime as dt
import json

import httpx

BASE = (
    "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "annonces-commerciales/records"
)

_EVENT_LABEL = {
    "creation": "création",
    "modification": "modification statutaire",
    "vente": "vente / cession de fonds",
    "collective": "procédure collective",
    "radiation": "radiation",
    "dpc": "dépôt des comptes",
    "divers": "annonce",
}


def _as_dict(v) -> dict:
    if isinstance(v, dict):
        return v
    try:
        return json.loads(v) if v else {}
    except (TypeError, ValueError):
        return {}


def _detail(a: dict) -> str | None:
    fam = a.get("familleavis")
    if fam == "collective":
        j = _as_dict(a.get("jugement"))
        return j.get("nature") or j.get("famille") or "jugement"
    if fam == "modification":
        m = _as_dict(a.get("modificationsgenerales"))
        return m.get("descriptif") or None
    if fam == "dpc":
        d = _as_dict(a.get("depot"))
        return d.get("typeDepot") or None
    if fam == "vente":
        v = _as_dict(a.get("vente"))
        # free text — usually names the fonds, the seller and/or the acquéreur
        return (
            v.get("descriptif")
            or v.get("nomActivite")
            or a.get("commercant")
            or "vente / cession de fonds"
        )
    return None


def summarize(annonces: list[dict]) -> dict:
    if not annonces:
        return {}
    annonces = sorted(
        annonces, key=lambda a: a.get("dateparution") or "", reverse=True
    )
    fams = [a.get("familleavis") for a in annonces]
    proc = next((a for a in annonces if a.get("familleavis") == "collective"), None)
    modif = next((a for a in annonces if a.get("familleavis") == "modification"), None)
    cutoff = (dt.date.today() - dt.timedelta(days=730)).isoformat()

    def _first_date(fam: str) -> str | None:
        # annonces is sorted most-recent first
        return next(
            (a.get("dateparution") for a in annonces if a.get("familleavis") == fam), None
        )

    return {
        "bodacc_en_procedure": proc is not None,
        "bodacc_procedure_detail": _detail(proc) if proc else None,
        "bodacc_procedure_date": proc.get("dateparution") if proc else None,
        "bodacc_a_depose_comptes": "dpc" in fams,
        "bodacc_derniere_annonce": annonces[0].get("dateparution"),
        "bodacc_derniere_modif_date": modif.get("dateparution") if modif else None,
        "bodacc_derniere_modif_detail": _detail(modif) if modif else None,
        "bodacc_vente_cession_date": _first_date("vente"),
        "bodacc_radiation_date": _first_date("radiation"),
        "bodacc_dernier_depot_date": _first_date("dpc"),
        "bodacc_nb_annonces_24m": sum(
            1 for a in annonces if (a.get("dateparution") or "") >= cutoff
        ),
        "bodacc_evenements": [
            {
                "date": a.get("dateparution"),
                "type": _EVENT_LABEL.get(a.get("familleavis"), a.get("familleavis")),
                "detail": _detail(a),
                "url": a.get("url_complete"),
            }
            for a in annonces[:6]
        ],
    }


def fetch_siren(
    siren: str, client: httpx.Client | None = None, limit: int = 30
) -> list[dict]:
    owns = client is None
    client = client or httpx.Client(timeout=8, headers={"Accept": "application/json"})
    try:
        r = client.get(
            BASE,
            params={
                "where": f'registre="{siren}"',
                "limit": limit,
                "order_by": "dateparution desc",
            },
        )
        if r.status_code != 200:
            return []
        return r.json().get("results", [])
    except (httpx.HTTPError, ValueError):
        return []
    finally:
        if owns:
            client.close()
