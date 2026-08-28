"""Client for recherche-entreprises.api.gouv.fr — dirigeants (RNE/INPI),
comptes, effectif, catégorie, Qualiopi/ESS. No auth, ~7 req/s per IP.

Used both on-demand by the serving layer (one SIREN) and by the batch
ingestion `ingestion.recherche_entreprises` (whole parc → Bronze).
"""

from __future__ import annotations

import httpx

BASE = "https://recherche-entreprises.api.gouv.fr"


def _person_name(d: dict) -> str | None:
    if d.get("denomination"):
        return d["denomination"]
    prenom = (d.get("prenoms") or "").split(" ")[0].title() or None
    nom = (d.get("nom") or "").title() or None
    return " ".join(x for x in (prenom, nom) if x) or None


def _dirigeant_label(d: dict) -> str | None:
    name = _person_name(d)
    if not name:
        return None
    q = d.get("qualite")
    return f"{name}, {q}" if q else name


def extract(result: dict) -> dict:
    """Flatten one /search result to the enrichment fields we keep."""
    comp = result.get("complements") or {}
    fin = result.get("finances") or {}
    year = max(fin) if fin else None
    f = (fin.get(year) or {}) if year else {}
    dirigeants = result.get("dirigeants") or []
    return {
        "siren": str(result.get("siren")) if result.get("siren") else None,
        "dirigeant_principal": _dirigeant_label(dirigeants[0]) if dirigeants else None,
        "dirigeants": [
            {
                "nom": _person_name(d),
                "qualite": d.get("qualite"),
                "type": d.get("type_dirigeant"),
            }
            for d in dirigeants
        ],
        "nb_dirigeants": len(dirigeants),
        "categorie_entreprise": result.get("categorie_entreprise"),
        "tranche_effectif_rne": result.get("tranche_effectif_salarie"),
        "annee_effectif_rne": result.get("annee_tranche_effectif_salarie"),
        "ca": f.get("ca"),
        "resultat_net": f.get("resultat_net"),
        "annee_comptes": int(year) if year else None,
        "nb_etablissements_ouverts": result.get("nombre_etablissements_ouverts"),
        "est_ess_rne": comp.get("est_ess"),
        "est_qualiopi": comp.get("est_qualiopi"),
        "est_organisme_formation": comp.get("est_organisme_formation"),
        "convention_collective": (comp.get("liste_idcc") or [None])[0],
        "adresse_rne": (result.get("siege") or {}).get("adresse"),
        "date_maj_rne": result.get("date_mise_a_jour_rne"),
    }


def fetch_one(siren: str, client: httpx.Client | None = None) -> dict | None:
    """Enrichment for one SIREN, or None if not found / API unavailable."""
    owns = client is None
    client = client or httpx.Client(timeout=8, headers={"Accept": "application/json"})
    try:
        r = client.get(f"{BASE}/search", params={"q": siren, "page": 1, "per_page": 1})
        if r.status_code != 200:
            return None
        for res in r.json().get("results", []):
            if str(res.get("siren")) == str(siren):
                return extract(res)
        return None
    except (httpx.HTTPError, ValueError):
        return None
    finally:
        if owns:
            client.close()
