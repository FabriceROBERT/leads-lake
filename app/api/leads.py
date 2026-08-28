from fastapi import APIRouter, HTTPException, Query, status

from app.controllers import leads_controller
from app.schemas.leads_schema import Lead, LeadsPage

router = APIRouter(prefix="/leads", tags=["Leads"])


@router.get("", response_model=LeadsPage)
async def list_leads(
    segment: str | None = Query(
        None, description="avocat | expert_comptable | paie | notaire"
    ),
    departement: str | None = Query(None, description="department code, e.g. 44"),
    score_min: float | None = Query(None, ge=0),
    has_recent_offer: bool | None = Query(
        None, description="filter on a job offer in the last 30 days"
    ),
    include_clients: bool = Query(
        False, description="include cabinets already Papperless clients"
    ),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Latest run of gold/leads_scored, filtered and ranked by score desc."""
    return leads_controller.list_leads(
        segment=segment,
        departement=departement,
        score_min=score_min,
        has_recent_offer=has_recent_offer,
        include_clients=include_clients,
        limit=limit,
        offset=offset,
    )


@router.get("/{siren}", response_model=Lead)
def get_lead(siren: str):
    lead = leads_controller.get_lead(siren)
    if lead is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Lead introuvable"
        )
    return lead
