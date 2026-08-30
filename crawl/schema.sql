-- Contact-crawl frontier. Applied idempotently at the start of every
-- `crawl_discovery` DAG run (see dags/crawl.py).

CREATE TABLE IF NOT EXISTS crawl_frontier (
    siren           TEXT PRIMARY KEY,
    priority        INTEGER      NOT NULL DEFAULT 0,   -- lead score, drives drain order
    raison_sociale  TEXT,
    commune         TEXT,
    domaine         TEXT,                              -- resolved website host, cached for life
    status          TEXT         NOT NULL DEFAULT 'pending',
        -- pending | resolved_verified | resolved_unverified | no_domain | crawl_failed | dead
    attempts        INTEGER      NOT NULL DEFAULT 0,
    discovered_at   TIMESTAMPTZ,
    last_crawled_at TIMESTAMPTZ,
    next_due_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS crawl_frontier_due_idx
    ON crawl_frontier (next_due_at, priority DESC)
    WHERE status IN ('resolved_verified', 'resolved_unverified');

CREATE INDEX IF NOT EXISTS crawl_frontier_status_idx
    ON crawl_frontier (status);
