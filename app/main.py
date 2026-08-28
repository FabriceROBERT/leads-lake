import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from app.api import health, kpis, leads, map as map_api
from app.config.settings import settings
from app.services.lake_service import LakeUnavailable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title=settings.api_title, version=settings.api_version)


class _PathAwareGZip(GZipMiddleware):
    """Skip gzip for vector tiles — they are already gzip-compressed inside the
    pmtiles archive and re-encoding would corrupt the Content-Encoding contract."""

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "http" and scope.get("path", "").startswith("/map/tiles/"):
            await self.app(scope, receive, send)
            return
        await super().__call__(scope, receive, send)


# level 6: ~2x faster than the default 9 for a few % larger output — worth it
# on the multi-MB /map/points payloads.
app.add_middleware(_PathAwareGZip, minimum_size=1000, compresslevel=6)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.front_url],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(LakeUnavailable)
async def lake_unavailable_handler(_request: Request, exc: LakeUnavailable):
    logger.error("Lake unavailable: %s", exc)
    return JSONResponse(
        status_code=503, content={"detail": f"Data lake unavailable: {exc}"}
    )


app.include_router(health.router)
app.include_router(leads.router)
app.include_router(kpis.router)
app.include_router(map_api.router)


@app.on_event("startup")
async def _prewarm_vector_tiles() -> None:
    """Walk the pmtiles directory into memory in the background so the first map
    tile request doesn't eat the ~3s cold load."""
    import asyncio

    from app.controllers.map_controller import _load_pmtiles

    asyncio.get_event_loop().run_in_executor(None, _load_pmtiles)
