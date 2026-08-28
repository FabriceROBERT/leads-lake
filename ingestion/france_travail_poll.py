"""Probe the France Travail 'Offres d'emploi v2' API — validate access + filters.

    python -m ingestion.france_travail_poll --since 2026-08-20T00:00:00Z
    python -m ingestion.france_travail_poll --rome M1203,K1902 --dump-schema

No writes. Just: get an OAuth token, run one /offres/search, print what comes back.
Needs FT_CLIENT_ID / FT_CLIENT_SECRET in .env.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone

import httpx

from app.config.settings import settings

# Best-guess ROME codes per target métier — VERIFY against the ROME referential.
TARGET_ROME = {
    "comptabilite": ["M1203"],           # collaborateur / assistant comptable, paie
    "rh_paie": ["M1501"],                # assistanat RH / gestionnaire de paie
    "juridique": ["K1902"],              # juriste, assistant juridique, clerc
    "transaction_immo": ["C1504"],       # négociateur immobilier (promoteurs)
    "projet_immo": ["C1503"],            # management de projet immobilier
    "patrimoine": ["C1206"],             # conseil en gestion de patrimoine (CGP)
}
DEFAULT_ROME = sorted({c for codes in TARGET_ROME.values() for c in codes})


def get_token() -> str:
    if not (settings.ft_client_id and settings.ft_client_secret):
        raise SystemExit("FT_CLIENT_ID / FT_CLIENT_SECRET missing from .env")
    # FT requires the scope to also carry application_<client_id>
    scope = f"application_{settings.ft_client_id} {settings.ft_scope}"
    resp = httpx.post(
        settings.ft_token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": settings.ft_client_id,
            "client_secret": settings.ft_client_secret,
            "scope": scope,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
        follow_redirects=True,
    )
    if resp.status_code != 200 or "json" not in resp.headers.get("content-type", ""):
        raise SystemExit(
            f"token request failed: HTTP {resp.status_code} "
            f"({resp.headers.get('content-type')})\nURL: {resp.url}\n{resp.text[:800]}"
        )
    body = resp.json()
    print(f"token OK (expires_in={body.get('expires_in')}s, scope={body.get('scope')})")
    return body["access_token"]


def search(token: str, params: dict) -> httpx.Response:
    return httpx.get(
        f"{settings.ft_api_base}/offres/search",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params=params,
        timeout=60,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rome", default=None, help="comma list of ROME codes")
    ap.add_argument("--naf", default=None, help="comma list of NAF codes of the hiring company, e.g. 69.20Z,69.10Z")
    ap.add_argument("--since", default=None, help="ISO datetime, default: 24h ago")
    ap.add_argument("--departement", default=None)
    ap.add_argument("--max", type=int, default=20, help="rows to fetch (range 0-N)")
    ap.add_argument("--dump-schema", action="store_true", help="print the full JSON of the first offer")
    args = ap.parse_args()

    since = args.since or (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "minCreationDate": since,
        "maxCreationDate": now,
        "sort": "1",  # by date, most recent first
        "range": f"0-{max(0, args.max - 1)}",
    }
    if args.rome:
        params["codeROME"] = args.rome
    if args.naf:
        params["codeNAF"] = args.naf
    if args.departement:
        params["departement"] = args.departement

    token = get_token()
    print(f"search {params}")
    resp = search(token, params)
    print(f"HTTP {resp.status_code}   Content-Range: {resp.headers.get('Content-Range')}")

    if resp.status_code not in (200, 206):
        print(resp.text[:1000])
        raise SystemExit(1)

    offres = resp.json().get("resultats", [])
    print(f"{len(offres)} offres dans la page\n")

    for o in offres:
        ent = o.get("entreprise", {}) or {}
        lieu = o.get("lieuTravail", {}) or {}
        print(
            f"  {o.get('dateCreation','?')[:16]}  {o.get('intitule','')[:48]:48}  "
            f"| {(ent.get('nom') or '—')[:28]:28} siret={ent.get('siret') or '—':14} "
            f"| {lieu.get('libelle') or '—'}  ROME={o.get('romeCode')}"
        )

    if args.dump_schema and offres:
        print("\n=== schéma de la 1ère offre ===")
        print(json.dumps(offres[0], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
