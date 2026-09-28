"""Durable sink. `event_id` is the idempotency key: ON CONFLICT DO NOTHING
means replaying the same message (at-least-once delivery, a consumer
crash after processing but before committing offsets, a producer retry)
never double-applies it. This is the source of truth for "was this event
actually processed" — the Redis check in
kafka_ops_lab/consumer/idempotency.py is a fast-path optimization in
front of it, not a replacement for it.
"""

import psycopg

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS deploy_events (
    event_id TEXT PRIMARY KEY,
    service TEXT NOT NULL,
    environment TEXT NOT NULL,
    status TEXT NOT NULL,
    released_by TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    duration_seconds DOUBLE PRECISION,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

UPSERT_SQL = """
INSERT INTO deploy_events (event_id, service, environment, status, released_by, occurred_at, duration_seconds)
VALUES (%(event_id)s, %(service)s, %(environment)s, %(status)s, %(released_by)s, %(occurred_at)s, %(duration_seconds)s)
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id;
"""


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()


def upsert_event(conn: psycopg.Connection, event: dict) -> bool:
    """Returns True if this call actually inserted a new row, False if the
    event_id already existed (duplicate delivery, correctly no-op'd)."""
    with conn.cursor() as cur:
        cur.execute(UPSERT_SQL, event)
        inserted = cur.fetchone() is not None
    conn.commit()
    return inserted


def count_events(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM deploy_events;")
        return cur.fetchone()[0]
