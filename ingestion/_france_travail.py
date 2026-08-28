"""Shared France Travail API helpers: OAuth token (cached) + offres search."""

from __future__ import annotations

import time

import httpx

from app.config.settings import settings

_token = {"value": None, "exp": 0.0}


def get_token() -> str:
    if _token["value"] and time.time() < _token["exp"] - 30:
        return _token["value"]  # type: ignore[return-value]
    if not (settings.ft_client_id and settings.ft_client_secret):
        raise RuntimeError("FT_CLIENT_ID / FT_CLIENT_SECRET missing from .env")
    scope = f"application_{settings.ft_client_id} {settings.ft_scope}"
    r = httpx.post(
        settings.ft_token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": settings.ft_client_id,
            "client_secret": settings.ft_client_secret,
            "scope": scope,
        },
        timeout=30,
        follow_redirects=True,
    )
    if r.status_code != 200 or "json" not in r.headers.get("content-type", ""):
        raise RuntimeError(f"FT token failed: HTTP {r.status_code} {r.text[:300]}")
    body = r.json()
    _token["value"] = body["access_token"]
    _token["exp"] = time.time() + int(body.get("expires_in", 1400))
    return _token["value"]  # type: ignore[return-value]


def search_offres(params: dict) -> tuple[list[dict], int]:
    """One /offres/search call. Returns (resultats, total_available)."""
    r = httpx.get(
        f"{settings.ft_api_base}/offres/search",
        headers={"Authorization": f"Bearer {get_token()}", "Accept": "application/json"},
        params=params,
        timeout=60,
    )
    if r.status_code == 204:
        return [], 0
    if r.status_code not in (200, 206):
        raise RuntimeError(f"FT search HTTP {r.status_code}: {r.text[:300]}")
    total = 0
    content_range = r.headers.get("Content-Range")  # "offres 0-149/382"
    if content_range and "/" in content_range:
        try:
            total = int(content_range.rsplit("/", 1)[-1])
        except ValueError:
            pass
    return r.json().get("resultats", []), total
