from pydantic import BaseModel, ConfigDict


class Lead(BaseModel):
    """One scored cabinet from gold/leads_scored.

    French snake_case for business attributes; English only for universal
    identifiers / coordinates / pipeline metadata (siren, siret, latitude,
    longitude, data_version, run_date). extra="allow": unknown Gold columns
    pass through untouched.
    """

    model_config = ConfigDict(extra="allow")

    siren: str | None = None
    siret: str | None = None
    raison_sociale: str | None = None
    segment: str | None = None
    code_ape: str | None = None
    categorie_juridique: str | int | None = None
    forme_juridique: str | None = None
    date_creation: str | None = None
    anciennete_annees: int | None = None
    tranche_effectif: str | None = None
    est_ess: bool | None = None

    adresse: str | None = None
    complement_adresse: str | None = None
    code_postal: str | None = None
    commune: str | None = None
    code_commune: str | None = None
    departement: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    ban_id: str | None = None
    est_diffusible: bool | None = None
    nb_etablissements: int | None = None

    nb_offres_30j: int | None = None
    nb_offres_90j: int | None = None
    nb_postes_30j: int | None = None
    nb_cdi_90j: int | None = None
    derniere_offre_date: str | None = None
    recence_jours: int | None = None
    metiers_recents: list[str] | None = None
    a_offre_paie: bool | None = None
    a_offre_comptabilite: bool | None = None
    a_offre_juridique: bool | None = None
    a_offre_patrimoine: bool | None = None
    a_offre_immobilier: bool | None = None
    derniere_offre_intitule: str | None = None
    derniere_offre_url: str | None = None

    score: int | None = None
    bande_score: str | None = None
    motifs_score: list[str] | None = None

    est_client: bool | None = None
    run_date: str | None = None

    # live enrichment — recherche-entreprises.api.gouv.fr (RNE/INPI)
    dirigeant_principal: str | None = None
    dirigeants: list[dict] | None = None
    nb_dirigeants: int | None = None
    categorie_entreprise: str | None = None
    tranche_effectif_rne: str | None = None
    annee_effectif_rne: str | None = None
    ca: int | None = None
    resultat_net: int | None = None
    annee_comptes: int | None = None
    ca_n1: int | None = None
    resultat_n1: int | None = None
    annee_comptes_n1: int | None = None
    ca_croissance_pct: float | None = None
    nb_etablissements_ouverts: int | None = None
    est_ess_rne: bool | None = None
    est_qualiopi: bool | None = None
    est_organisme_formation: bool | None = None
    convention_collective: str | None = None
    adresse_rne: str | None = None
    date_maj_rne: str | None = None

    # live enrichment — BODACC (annonces légales)
    bodacc_en_procedure: bool | None = None
    bodacc_procedure_detail: str | None = None
    bodacc_procedure_date: str | None = None
    bodacc_a_depose_comptes: bool | None = None
    bodacc_derniere_annonce: str | None = None
    bodacc_derniere_modif_date: str | None = None
    bodacc_derniere_modif_detail: str | None = None
    bodacc_vente_cession_date: str | None = None
    bodacc_radiation_date: str | None = None
    bodacc_dernier_depot_date: str | None = None
    bodacc_nb_annonces_24m: int | None = None
    bodacc_evenements: list[dict] | None = None

    # scoring V2 — derived in silver/enrichissement + gold/leads_scored
    flags: list[str] | None = None
    score_detail: dict | None = None

    # contact crawl — silver/contacts
    telephone: str | None = None
    email: str | None = None
    site_web: str | None = None
    contact_verifie: bool | None = None


class LeadsPage(BaseModel):
    available: bool
    total: int
    limit: int
    offset: int
    items: list[Lead]


class KpiResponse(BaseModel):
    name: str
    available: bool
    rows: list[dict]
