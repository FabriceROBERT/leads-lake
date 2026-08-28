from fastapi import APIRouter, HTTPException, status

from app.controllers import leads_controller
from app.schemas.leads_schema import KpiResponse

router = APIRouter(prefix="/kpis", tags=["KPIs"])


@router.get("/{name}", response_model=KpiResponse)
async def get_kpi(name: str):
    """Read a gold/<name> KPI table (kpi_marche, kpi_signaux_du_jour, kpi_couverture)."""
    try:
        return leads_controller.get_kpi(name)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
        ) from exc
