from fastapi import APIRouter, status

from app.config.settings import settings
from app.services.lake_service import LakeUnavailable, read_dataset

router = APIRouter(tags=["Health"])


@router.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    """Liveness + lake reachability, for Docker healthcheck and monitoring."""
    lake = "reachable"
    healthy = True
    try:
        read_dataset(settings.gold_leads_path)  # empty df is fine, only errors matter
    except LakeUnavailable as exc:
        healthy = False
        lake = str(exc)

    return {
        "status": "healthy" if healthy else "degraded",
        "service": "leads-lake-api",
        "env": settings.env,
        "lake_root": settings.lake_root,
        "lake": lake,
    }


@router.get("/", status_code=status.HTTP_200_OK)
async def root():
    return {
        "message": "Leads Lake API - Gold serving layer for the Papperless lead datalake",
        "version": settings.api_version,
        "docs": "/docs",
        "health": "/health",
    }
