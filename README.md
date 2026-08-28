# Papperless Leads — data lake & API

Back end de génération de leads pour **Papperless**. Il identifie et score les
cabinets français de services professionnels (avocats / notaires,
experts-comptables, gestionnaires de paie, CGP, promoteurs, sociétés de
domiciliation) comme prospects pour l'offre de dématérialisation Papperless, à
partir de données publiques uniquement.

Ce dépôt contient **tout le back end** : les scripts d'ingestion, les jobs Spark
Bronze / Silver / Gold, les DAGs Airflow qui les orchestrent, et l'API FastAPI
que lit le front (`leads-lake-front`).

```
Sources ──► Bronze ──► Silver ──► Gold (leads_scored, kpi_*) ──► FastAPI ──► leads-lake-front
 SIRENE                                    parquet sur Wasabi/S3           (carte, carrousel, fiches)
 France Travail (flux Kafka)
 Nomenclature INSEE
 recherche-entreprises  ┐ enrichissement à la volée dans GET /leads/{siren}
 BODACC                 ┘ (hors pipeline médaillon)
```

## Sources de données

| Source | Usage | Auth |
| --- | --- | --- |
| **SIRENE** (stock établissements / unités légales) | parc des cabinets, adresses, NAF, effectif | clé INSEE (fichier stock : sans clé) |
| **France Travail** offres d'emploi | signal — un cabinet qui recrute paie/compta/juridique se réchauffe | client id/secret, streamé via Kafka |
| **Nomenclature INSEE** catégories juridiques | libeller le code `categorie_juridique` | aucune |
| **recherche-entreprises.api.gouv.fr** | dirigeants (RNE/INPI), comptes, CA, effectif RNE, Qualiopi, ESS | aucune, ~7 req/s |
| **BODACC** | procédures collectives, dépôts de comptes, événements « pourquoi maintenant » | aucune |

Les deux dernières sont appelées **à la volée** par SIREN à l'ouverture d'une
fiche (`leads_controller._enrich`, cache 24 h, tolérant aux pannes). Des scripts
d'ingestion Bronze + tâches DAG existent aussi pour elles, mais aucun job Silver
ne consomme encore ce Bronze.

## Arborescence

| Chemin | Rôle |
| --- | --- |
| `app/` | API FastAPI (`config`, `api`, `controllers`, `services`, `schemas`) |
| `ingestion/` | clients de sources + chargeurs batch → Bronze (`sirene_stock`, `recherche_entreprises`, `bodacc`, `insee_categories_juridiques`, pollers/producers France Travail) |
| `jobs/` | jobs Spark : `silver_cabinet`, `silver_offre_emploi`, `gold_leads_scored`, `gold_cabinet_zone`, `stream_france_travail`, `build_tiles` |
| `dags/` | DAGs Airflow : `ingestion_batch`, `silver`, `gold`, `france_travail` (+ `_lib.py`, helpers de tâches) |
| `docker/` | images : `airflow`, `spark`, `py` (tâches Python), `tiles` (tippecanoe) |
| `scripts/` | outils locaux : `seed_sample_gold`, `peek_bronze`, `peek_silver`, `clean_bronze` |
| `airflow/` | home Airflow (config, logs) |

## API de service

| Route | Rôle |
| --- | --- |
| `GET /health` | liveness + accessibilité du lake |
| `GET /leads?segment=&departement=&score_min=&has_recent_offer=&limit=&offset=` | dernier run, trié par score |
| `GET /leads/{siren}` | un lead + enrichissement à la volée (recherche-entreprises + BODACC) |
| `GET /map/points?format=geojson\|bulk\|count\|breakdown&<filtres>` | points / comptages / répartition |
| `GET /map/clusters?bbox=&zoom=&<filtres>` | clustering grille côté serveur (bulles numérotées) |
| `GET /map/tiles/{z}/{x}/{y}.mvt` | tuiles vectorielles depuis `gold/tiles/leads.pmtiles` (dormant) |
| `GET /map/zones` | agrégats par département / zone |
| `GET /firms/{siren}` | tous les établissements d'une entreprise (vue réseau) |
| `GET /kpis/{name}` | `kpi_marche`, `kpi_signaux_du_jour`, `kpi_couverture` |
| `GET /signaux-du-jour` | mouvements du jour |

Filtres carte communs : `segment` (liste séparée par virgules), `code_ape`,
`poste` (paie / comptabilite / juridique / patrimoine / immobilier), `bande`
(chaud / tiede / froid), `reseau` (mono / multi), `region` (code INSEE, DROM
inclus), `score_min`, `bbox`, `zoom`.

## Démarrage rapide (local, API seule)

```bash
python -m venv .venv
.venv\Scripts\activate               # Windows
pip install -r requirements-dev.txt

copy .env.example .env               # garder LAKE_ROOT=./_lake

python scripts/seed_sample_gold.py   # petit Gold d'exemple
uvicorn app.main:app --reload
```

- Docs : http://localhost:8000/docs
- `pytest` pour la suite de tests

## Pointer vers Wasabi

Dans `.env` :

```ini
LAKE_ROOT=s3://papperlesspreprod-leads-lake
S3_ENDPOINT_URL=https://s3.eu-west-2.wasabisys.com
S3_REGION=eu-west-2
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

Aucun changement de code — `settings.storage_options` alimente s3fs. Utiliser une
clé limitée à ce seul bucket et un `.env` distinct par environnement. `ENV` doit
valoir `local`, `staging` ou `production`.

## Pipeline (Spark + Airflow, local)

```bash
docker network create leads-lake-net
docker build -f docker/py.Dockerfile -t leads-lake-py:latest .
docker compose -f docker-compose.spark.yml --profile tools build
docker compose -f docker-compose.spark.yml up -d kafka
docker compose -f docker-compose.airflow.yml up -d
```

UI Airflow sur http://localhost:8080 (admin / admin). DAGs :

- `ingestion_batch` (`@monthly`) — SIRENE + CJ INSEE + recherche-entreprises +
  BODACC → Bronze, puis déclenche `silver` → `gold`.
- `silver` — `silver_cabinet` (parc + reprojection DOM-TOM),
  `silver_offre_emploi`.
- `gold` — `gold_leads_scored` (scoring + `nb_etablissements`),
  `gold_cabinet_zone`, puis `build_tiles`.
- `france_travail` — flux Kafka continu des offres → Bronze.

Idempotence via marqueurs `_SUCCESS` + partitions `data_version` / `run_date`.

## Déploiement (0 €)

Voir **[DEPLOY.md](DEPLOY.md)** — tout sur une seule VM ARM Oracle Cloud Always
Free : Caddy (HTTPS auto) → front Next + FastAPI (`docker-compose.prod.yml`), plus
Airflow / Kafka / Spark (`docker-compose.airflow.yml`,
`docker-compose.spark.yml`). Le data lake reste sur Wasabi.

- Front + API sur un seul domaine, Caddy route par chemin → pas de CORS.
- Airflow / Kafka ne sont **jamais exposés** ; y accéder par tunnel SSH
  (`ssh -N -L 8080:localhost:8080 ubuntu@<vm>`).
