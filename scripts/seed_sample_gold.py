"""Seed a small sample gold/leads_scored dataset so the API is demoable locally.

    python scripts/seed_sample_gold.py

Writes to ./_lake/gold/leads_scored/run_date=<today>/part-0.parquet
"""

from datetime import date
from pathlib import Path

import pandas as pd

RUN = date.today().isoformat()
LAKE = Path(__file__).resolve().parent.parent / "_lake"
OUT = LAKE / "gold" / "leads_scored" / f"run_date={RUN}"


ROWS = [
    {
        "siren": "812345678",
        "raison_sociale": "CABINET DURAND & ASSOCIES",
        "segment": "avocat",
        "code_ape": "6910Z",
        "forme_juridique": "SELARL",
        "tranche_effectif": "6 a 9",
        "date_creation": "2018-03-01",
        "commune": "Nantes",
        "code_postal": "44000",
        "departement": "44",
        "latitude": 47.2184,
        "longitude": -1.5536,
        "score": 88,
        "motifs_score": [
            "recrute un gestionnaire de paie",
            "profil proche du client CABINET X",
        ],
        "nb_offres_30j": 2,
        "nb_offres_90j": 3,
        "a_offre_paie": True,
        "a_offre_juridique": True,
        "a_offre_comptabilite": False,
        "derniere_offre_date": RUN,
        "ca": 640000.0,
        "resultat_net": 78000.0,
        "est_client": False,
    },
    {
        "siren": "902345671",
        "raison_sociale": "EXPERTISE COMPTABLE LOIRE",
        "segment": "expert_comptable",
        "code_ape": "6920Z",
        "forme_juridique": "SAS",
        "tranche_effectif": "10 a 19",
        "date_creation": "2012-09-15",
        "commune": "Angers",
        "code_postal": "49000",
        "departement": "49",
        "latitude": 47.4784,
        "longitude": -0.5632,
        "score": 72,
        "motifs_score": ["croissance d'effectif +3 sur 12 mois"],
        "nb_offres_30j": 0,
        "nb_offres_90j": 1,
        "a_offre_paie": False,
        "a_offre_juridique": False,
        "a_offre_comptabilite": True,
        "derniere_offre_date": None,
        "ca": 1250000.0,
        "resultat_net": 140000.0,
        "est_client": False,
    },
    {
        "siren": "753210987",
        "raison_sociale": "PAIE & RH CONSEIL",
        "segment": "paie",
        "code_ape": "7022Z",
        "forme_juridique": "SARL",
        "tranche_effectif": "3 a 5",
        "date_creation": "2021-01-10",
        "commune": "Rennes",
        "code_postal": "35000",
        "departement": "35",
        "latitude": 48.1173,
        "longitude": -1.6778,
        "score": 64,
        "motifs_score": ["cabinet cree il y a moins de 5 ans"],
        "nb_offres_30j": 1,
        "nb_offres_90j": 1,
        "a_offre_paie": True,
        "a_offre_juridique": False,
        "a_offre_comptabilite": False,
        "derniere_offre_date": RUN,
        "ca": None,
        "resultat_net": None,
        "est_client": False,
    },
    {
        "siren": "444555666",
        "raison_sociale": "SCP NOTAIRES DE L'OUEST",
        "segment": "notaire",
        "code_ape": "6910Z",
        "forme_juridique": "SCP",
        "tranche_effectif": "20 a 49",
        "date_creation": "2005-06-20",
        "commune": "Vannes",
        "code_postal": "56000",
        "departement": "56",
        "latitude": 47.6582,
        "longitude": -2.7608,
        "score": 41,
        "motifs_score": ["aucun signal recent"],
        "nb_offres_30j": 0,
        "nb_offres_90j": 0,
        "a_offre_paie": False,
        "a_offre_juridique": False,
        "a_offre_comptabilite": False,
        "derniere_offre_date": None,
        "ca": 2100000.0,
        "resultat_net": 260000.0,
        "est_client": True,
    },
]


KPI_MARCHE = [
    {"segment": "avocat", "departement": "44", "nb_cabinets": 320, "nb_leads": 210},
    {"segment": "expert_comptable", "departement": "49", "nb_cabinets": 210, "nb_leads": 140},
    {"segment": "paie", "departement": "35", "nb_cabinets": 90, "nb_leads": 65},
]

KPI_SIGNAUX = [
    {"siren": "812345678", "raison_sociale": "CABINET DURAND & ASSOCIES", "signal": "offre paie", "date": RUN},
    {"siren": "753210987", "raison_sociale": "PAIE & RH CONSEIL", "signal": "offre paie", "date": RUN},
]


def _write(rows: list[dict], *parts: str) -> None:
    out = LAKE.joinpath(*parts)
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(out / "part-0.parquet", index=False)
    print(f"Wrote {len(rows)} rows -> {out / 'part-0.parquet'}")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(ROWS).to_parquet(OUT / "part-0.parquet", index=False)
    print(f"Wrote {len(ROWS)} rows -> {OUT / 'part-0.parquet'}")
    _write(KPI_MARCHE, "gold", "kpi_marche")
    _write(KPI_SIGNAUX, "gold", "kpi_signaux_du_jour")


if __name__ == "__main__":
    main()
