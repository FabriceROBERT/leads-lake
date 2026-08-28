from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from app.config.settings import settings

RUN = date.today().isoformat()

LEADS = [
    {
        "siren": "812345678",
        "raison_sociale": "CABINET DURAND",
        "segment": "avocat",
        "departement": "44",
        "score": 88,
        "nb_offres_30j": 2,
        "est_client": False,
        "motifs_score": ["recrute un gestionnaire de paie"],
    },
    {
        "siren": "902345671",
        "raison_sociale": "EC LOIRE",
        "segment": "expert_comptable",
        "departement": "49",
        "score": 72,
        "nb_offres_30j": 0,
        "est_client": False,
        "motifs_score": ["croissance d'effectif"],
    },
    {
        "siren": "444555666",
        "raison_sociale": "SCP NOTAIRES",
        "segment": "notaire",
        "departement": "56",
        "score": 41,
        "nb_offres_30j": 0,
        "est_client": True,
        "motifs_score": [],
    },
]

KPI_MARCHE = [
    {"segment": "avocat", "departement": "44", "nb_cabinets": 320},
    {"segment": "expert_comptable", "departement": "49", "nb_cabinets": 210},
]


def _write_sample(root: Path) -> None:
    leads_dir = root / "gold" / "leads_scored" / f"run_date={RUN}"
    leads_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(LEADS).to_parquet(leads_dir / "part-0.parquet", index=False)

    kpi_dir = root / "gold" / "kpi_marche"
    kpi_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(KPI_MARCHE).to_parquet(kpi_dir / "part-0.parquet", index=False)


@pytest.fixture
def sample_lake(tmp_path, monkeypatch):
    _write_sample(tmp_path)
    monkeypatch.setattr(settings, "lake_root", str(tmp_path))
    return tmp_path


@pytest.fixture
def empty_lake(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "lake_root", str(tmp_path))
    return tmp_path
