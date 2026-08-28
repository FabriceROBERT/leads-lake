"""Probe the France Travail 'Marché du travail' API — check access + see shape.

    python -m ingestion.marche_travail_poll
"""

from __future__ import annotations

import json
import time

import httpx

from app.config.settings import settings

SCOPES = [
    "api_marche-du-travailv1",
    "api_stats-perspectives-emploiv1",
    "api_stats-offres-demandes-emploiv1",
    "api_marchetravailv1",
    "api_stats-du-marche-du-travailv1",
]
BASE = "https://api.francetravail.io/partenaire/marche-travail/v1"


def token_for(scope: str):
    r = httpx.post(
        settings.ft_token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": settings.ft_client_id,
            "client_secret": settings.ft_client_secret,
            "scope": f"application_{settings.ft_client_id} {scope}",
        },
        timeout=30,
        follow_redirects=True,
    )
    if r.status_code == 200 and "json" in r.headers.get("content-type", ""):
        return True, r.json()["access_token"]
    return False, f"HTTP {r.status_code} {r.text[:160]}"


def main() -> None:
    token = used = None
    for sc in SCOPES:
        ok, res = token_for(sc)
        print(f"scope {sc!r:38} -> {'OK' if ok else res}")
        if ok and token is None:
            token, used = res, sc
        time.sleep(0.3)
    if token is None:
        raise SystemExit("no working scope for Marché du travail")

    print(f"\nusing scope {used!r}")
    h = {"Authorization": f"Bearer {token}", "Accept": "application/json", "Content-Type": "application/json"}

    # GET probes
    for path in ["", "/indicateur", "/indicateurs", "/referentiel/indicateurs", "/referentiel/territoires"]:
        r = httpx.get(BASE + path, headers=h, timeout=40)
        print(f"GET  {path or '/':32} -> {r.status_code}  {r.text[:200]!r}")
        time.sleep(0.3)

    # POST probe: national ROME M1203 (comptabilité), last period
    body = {
        "codeTypeTerritoire": "NAT",
        "codeTerritoire": "FR",
        "codeTypeActivite": "ROME",
        "codeActivite": "M1203",
        "codeTypePeriode": "ANNEE",
        "dernierePeriode": True,
    }
    for path in ["/indicateur/stat-offres", "/indicateur/stat-demandes", "/stat-offres"]:
        r = httpx.post(BASE + path, headers=h, json=body, timeout=40)
        print(f"POST {path:32} -> {r.status_code}  {r.text[:300]!r}")
        if r.status_code == 200:
            print(json.dumps(r.json(), indent=2, ensure_ascii=False)[:1500])
        time.sleep(0.3)


if __name__ == "__main__":
    main()
