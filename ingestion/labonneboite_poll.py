"""Probe the France Travail 'La Bonne Boite' API — check access + see the schema.

    python -m ingestion.labonneboite_poll
    python -m ingestion.labonneboite_poll --lat 48.8566 --lon 2.3522 --distance 30 --rome M1203 --naf 69.20Z --dump

La Bonne Boite is conditional-access: a 403 "invalid_scope" means France Travail
has not validated the authorization for your app yet.
"""

from __future__ import annotations

import argparse
import json
import time

import httpx

from app.config.settings import settings

# candidate scopes to try (LBB scope name has varied across the migration)
SCOPES = ["api_labonneboitev1", "api_labonneboitev2", "api_labonneboitev1 o2dsoffre"]
BASE = "https://api.francetravail.io/partenaire/labonneboite/v1"


def token_for(scope: str) -> tuple[bool, str]:
    full = f"application_{settings.ft_client_id} {scope}"
    r = httpx.post(
        settings.ft_token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": settings.ft_client_id,
            "client_secret": settings.ft_client_secret,
            "scope": full,
        },
        timeout=30,
        follow_redirects=True,
    )
    if r.status_code == 200 and "json" in r.headers.get("content-type", ""):
        return True, r.json()["access_token"]
    return False, f"HTTP {r.status_code} {r.text[:200]}"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lat", type=float, default=48.8566)
    ap.add_argument("--lon", type=float, default=2.3522)
    ap.add_argument("--distance", type=int, default=30)
    ap.add_argument("--rome", default="M1203")
    ap.add_argument("--naf", default="69.20Z")
    ap.add_argument("--page-size", type=int, default=10)
    ap.add_argument("--dump", action="store_true", help="print full JSON of the first company")
    args = ap.parse_args()

    token = None
    for sc in SCOPES:
        ok, res = token_for(sc)
        print(f"scope {sc!r:35} -> {'OK' if ok else res}")
        if ok and token is None:
            token, used_scope = res, sc
        time.sleep(0.3)
    if token is None:
        raise SystemExit("no working scope — La Bonne Boite likely not validated for this app")

    print(f"\nusing scope {used_scope!r}")
    params = {
        "latitude": args.lat,
        "longitude": args.lon,
        "distance": args.distance,
        "rome_codes": args.rome,
        "page": 1,
        "page_size": args.page_size,
        "sort": "score",
    }
    if args.naf:
        params["naf_codes"] = args.naf

    r = httpx.get(
        f"{BASE}/company/",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params=params,
        timeout=60,
    )
    print(f"GET {r.request.url}\nHTTP {r.status_code}")
    if r.status_code != 200:
        print(r.text[:1200])
        raise SystemExit(1)

    body = r.json()
    companies = body.get("companies", [])
    print(f"companies_count={body.get('companies_count')}   page={len(companies)}\n")
    for c in companies:
        print(
            f"  {str(c.get('stars','?')):4} {(c.get('name') or '')[:36]:36} "
            f"siret={c.get('siret','—'):15} naf={c.get('naf','—'):8} {c.get('city','')}"
        )
    if args.dump and companies:
        print("\n=== schéma 1ère entreprise ===")
        print(json.dumps(companies[0], indent=2, ensure_ascii=False))
    print("\nclés réponse:", sorted(body.keys()))


if __name__ == "__main__":
    main()
