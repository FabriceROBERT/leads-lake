"""Tiny psycopg helper for the contact-crawl frontier DB.

Connection string comes from settings.crawl_db_url (env CRAWL_DB_URL), e.g.
`postgresql://crawl:crawl@crawl-db:5432/crawl`.
"""

from __future__ import annotations

from pathlib import Path

import psycopg

from app.config.settings import settings

_SCHEMA = Path(__file__).resolve().parents[1] / "crawl" / "schema.sql"


def connect() -> psycopg.Connection:
    return psycopg.connect(settings.crawl_db_url, autocommit=True)


def ensure_schema() -> None:
    """Apply crawl/schema.sql (idempotent: CREATE TABLE / INDEX IF NOT EXISTS)."""
    sql = _SCHEMA.read_text(encoding="utf-8")
    with connect() as conn:
        conn.execute(sql)


def known_sirens() -> set[str]:
    with connect() as conn:
        return {r[0] for r in conn.execute("SELECT siren FROM crawl_frontier")}


def seed_pending(rows: list[dict]) -> int:
    """Insert freshly-seen sirens as `pending`. rows: {siren, priority, raison_sociale, commune}.
    Existing sirens are left untouched (priority is refreshed though)."""
    if not rows:
        return 0
    with connect() as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO crawl_frontier (siren, priority, raison_sociale, commune)
            VALUES (%(siren)s, %(priority)s, %(raison_sociale)s, %(commune)s)
            ON CONFLICT (siren) DO UPDATE SET
                priority = EXCLUDED.priority,
                raison_sociale = COALESCE(crawl_frontier.raison_sociale, EXCLUDED.raison_sociale),
                commune = COALESCE(crawl_frontier.commune, EXCLUDED.commune),
                updated_at = now()
            """,
            rows,
        )
        return cur.rowcount


def pending_for_discovery(limit: int | None = None) -> list[dict]:
    q = (
        "SELECT siren, raison_sociale, commune FROM crawl_frontier "
        "WHERE status = 'pending' ORDER BY priority DESC"
    )
    if limit:
        q += f" LIMIT {int(limit)}"
    with connect() as conn:
        return [
            {"siren": s, "raison_sociale": rs, "commune": c}
            for s, rs, c in conn.execute(q)
        ]


def set_discovery_result(siren: str, domaine: str | None) -> None:
    status = "resolved_unverified" if domaine else "no_domain"
    with connect() as conn:
        conn.execute(
            """
            UPDATE crawl_frontier SET
                domaine = %s,
                status = %s,
                attempts = attempts + 1,
                discovered_at = now(),
                updated_at = now()
            WHERE siren = %s
            """,
            (domaine, status, siren),
        )


def due_for_crawl(limit: int | None = None) -> list[dict]:
    q = (
        "SELECT siren, domaine, priority FROM crawl_frontier "
        "WHERE domaine IS NOT NULL AND next_due_at <= now() "
        "AND status IN ('resolved_verified', 'resolved_unverified', 'crawl_failed') "
        "ORDER BY priority DESC"
    )
    if limit:
        q += f" LIMIT {int(limit)}"
    with connect() as conn:
        return [
            {"siren": s, "domaine": d, "priority": p}
            for s, d, p in conn.execute(q)
        ]


def set_crawl_result(siren: str, status: str, next_due_days: int) -> None:
    with connect() as conn:
        conn.execute(
            """
            UPDATE crawl_frontier SET
                status = %s,
                attempts = attempts + 1,
                last_crawled_at = now(),
                next_due_at = now() + make_interval(days => %s),
                updated_at = now()
            WHERE siren = %s
            """,
            (status, next_due_days, siren),
        )
