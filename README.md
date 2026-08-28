# leads-lake

Serving API for the **Papperless lead datalake**. It exposes the **Gold** layer
(`leads_scored` + KPIs) over REST so an internal dashboard — or a page in the
Papperless admin portal — can read scored cabinets (avocats, experts-comptables,
gestionnaires de paie, notaires) without touching Spark or the raw lake.

This repo is **only the read/serving layer**. Ingestion (SIRENE bulk,
recherche-entreprises, France Travail → Kafka), the Bronze/Silver/Gold Spark jobs
and the Airflow DAGs live in a separate project.

```
Sources ─► Bronze ─► Silver ─► Gold (leads_scored, kpi_*)  ──►  [ this API ]  ──►  dashboard / admin UI
                                        (parquet on HDFS or Wasabi/S3)
```

## Stack

FastAPI · pandas + pyarrow · s3fs (Wasabi/S3) — same conventions as
`papperless-back` (`app/config`, `app/api`, `app/controllers`, `app/services`,
`app/schemas`).

## Layout

| Path | Role |
| --- | --- |
| `app/config/settings.py` | env-driven config; `LAKE_ROOT` selects local vs S3 |
| `app/services/lake_service.py` | read parquet datasets from the lake, tolerate "not produced yet" |
| `app/controllers/leads_controller.py` | filter / rank / shape Gold records |
| `app/api/` | routers: `health`, `leads`, `kpis` |
| `scripts/seed_sample_gold.py` | write a tiny sample `gold/leads_scored` for local demo |

## Quickstart (local)

```bash
python -m venv .venv
.venv\Scripts\activate            # Windows
pip install -r requirements-dev.txt

copy .env.example .env            # keep LAKE_ROOT=./_lake

python scripts/seed_sample_gold.py
uvicorn app.main:app --reload
```

- Docs: http://localhost:8000/docs
- `GET /health` — liveness + lake reachability
- `GET /leads?segment=avocat&has_recent_offer=true&score_min=60` — latest run, ranked by score
- `GET /leads/{siren}`
- `GET /kpis/kpi_signaux_du_jour` — one of `kpi_marche`, `kpi_signaux_du_jour`, `kpi_couverture`

```bash
pytest
```

## Pointing at Wasabi

In `.env`:

```
LAKE_ROOT=s3://papperlesspreprod-leads-lake
S3_ENDPOINT_URL=https://s3.eu-west-2.wasabisys.com
S3_REGION=eu-west-2
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

No code change — `settings.storage_options` feeds s3fs. Use a key scoped to that
one bucket, and a separate `.env` per environment (`...-preprod`, `...-prod`).

## Docker

```bash
docker compose up --build
```

## Not in scope here (separate datalake repo)

- Kafka + Spark Structured Streaming (France Travail feed)
- Bronze/Silver/Gold Spark jobs
- Airflow DAGs (ingestion / Silver / Gold) + idempotence (`_SUCCESS`, watermarks)
- Export of `leads_scored` into a `crm_leads` table consumed by Papperless
