from fastapi import APIRouter, Query, Response

from app.controllers import map_controller

router = APIRouter(tags=["Map"])


@router.get("/map/points")
def map_points(
    bbox: str | None = Query(None, description="minLon,minLat,maxLon,maxLat"),
    segment: str | None = Query(None, description="segment(s), séparés par des virgules (avocat_notaire, expert_comptable, cgp, promoteur, domiciliation)"),
    code_ape: str | None = Query(None, description="Code(s) NAF exacts, séparés par des virgules (ex: 41.10A,41.10B)"),
    poste: str | None = Query(
        None,
        pattern="^(any|paie|comptabilite|juridique|patrimoine|immobilier)$",
        description="any = au moins une offre sur 90 j ; sinon métier recherché",
    ),
    bande: str | None = Query(None, description="chaud | tiede | froid"),
    reseau: str | None = Query(
        None, pattern="^(mono|multi)$", description="mono = 1 établissement ; multi = réseau"
    ),
    region: str | None = Query(None, description="Code région INSEE (11, 24, …) — filtre par départements"),
    score_min: float | None = Query(None, ge=0),
    limit: int = Query(5000, ge=1, le=300000),
    format: str = Query(
        "geojson",
        pattern="^(geojson|bulk|count|breakdown)$",
        description="bulk = lean columnar ; count = {count} ; breakdown = counts by bande/segment/dept",
    ),
):
    """Scored leads with coordinates (latest run) — GeoJSON, bulk, count, or breakdown."""
    return map_controller.map_points(
        bbox=bbox,
        segment=segment,
        code_ape=code_ape,
        poste=poste,
        bande=bande,
        reseau=reseau,
        region=region,
        score_min=score_min,
        limit=limit,
        bulk=format == "bulk",
        count_only=format == "count",
        breakdown_only=format == "breakdown",
    )


@router.get("/map/clusters")
async def map_clusters(
    bbox: str = Query(..., description="minLon,minLat,maxLon,maxLat"),
    zoom: float = Query(..., ge=0, le=22),
    segment: str | None = Query(None),
    code_ape: str | None = Query(None),
    poste: str | None = Query(
        None, pattern="^(any|paie|comptabilite|juridique|patrimoine|immobilier)$"
    ),
    bande: str | None = Query(None),
    reseau: str | None = Query(None, pattern="^(mono|multi)$"),
    region: str | None = Query(None),
    score_min: float | None = Query(None, ge=0),
):
    """Grid-aggregated leads for the viewport: numbered clusters + singleton points."""
    return map_controller.map_clusters(
        bbox=bbox,
        zoom=zoom,
        segment=segment,
        code_ape=code_ape,
        poste=poste,
        bande=bande,
        reseau=reseau,
        region=region,
        score_min=score_min,
    )


@router.get("/map/tiles/{z}/{x}/{y}.mvt")
def vector_tile(z: int, x: int, y: int):
    """One MVT vector tile from gold/tiles/leads.pmtiles (built by jobs/build_tiles.py)."""
    tile = map_controller.vector_tile(z, x, y)
    if tile is None:
        return Response(status_code=204)
    data, encoding = tile
    headers = {"Cache-Control": "public, max-age=300"}
    if encoding:
        headers["Content-Encoding"] = encoding
    return Response(
        content=data,
        media_type="application/vnd.mapbox-vector-tile",
        headers=headers,
    )


@router.get("/firms/{siren}")
def firm_network(siren: str):
    """Every establishment (siège + secondaires) of one firm, for the linked map view."""
    return map_controller.firm_network(siren)


@router.get("/map/zones")
def map_zones(
    level: str = Query("departement", pattern="^(departement|commune)$"),
    segment: str | None = None,
):
    """Cabinet counts per departement/commune × segment, for the zoomed-out layer."""
    return map_controller.map_zones(level=level, segment=segment)


@router.get("/signaux-du-jour")
def signaux_du_jour(limit: int = Query(200, ge=1, le=1000)):
    """Fresh actionable leads (recence <= 2 j), ranked by score."""
    return map_controller.signaux_du_jour(limit=limit)
