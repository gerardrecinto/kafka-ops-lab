import uuid

from kafka_ops_lab.sink.postgres_sink import count_events, upsert_event


def make_event(event_id: str) -> dict:
    return {
        "event_id": event_id,
        "service": "service-01",
        "environment": "prod",
        "status": "succeeded",
        "released_by": "engineer-1",
        "occurred_at": "2026-09-28T00:00:00+00:00",
        "duration_seconds": 120.5,
    }


def test_upsert_inserts_new_event(pg_conn):
    event_id = str(uuid.uuid4())
    inserted = upsert_event(pg_conn, make_event(event_id))
    assert inserted is True
    assert count_events(pg_conn) == 1


def test_upsert_is_idempotent_on_duplicate_event_id(pg_conn):
    event_id = str(uuid.uuid4())
    event = make_event(event_id)

    first = upsert_event(pg_conn, event)
    second = upsert_event(pg_conn, event)

    assert first is True
    assert second is False
    assert count_events(pg_conn) == 1
