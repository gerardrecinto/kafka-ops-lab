import json
import uuid

import pytest

from kafka_ops_lab.consumer.pipeline import PoisonMessageError, TransientProcessingError, process_message
from kafka_ops_lab.sink.postgres_sink import count_events


def make_raw_event(event_id: str) -> bytes:
    return json.dumps(
        {
            "schema_version": "1.1",
            "event_id": event_id,
            "service": "service-01",
            "environment": "prod",
            "status": "succeeded",
            "released_by": "engineer-1",
            "occurred_at": "2026-09-28T00:00:00+00:00",
            "duration_seconds": 42.0,
        }
    ).encode("utf-8")


def test_valid_message_is_inserted(redis_client, pg_conn):
    result = process_message(make_raw_event(str(uuid.uuid4())), redis_client, pg_conn)
    assert result.outcome == "inserted"
    assert count_events(pg_conn) == 1


def test_redelivered_message_is_deduped_by_redis_before_hitting_postgres(redis_client, pg_conn):
    raw = make_raw_event(str(uuid.uuid4()))
    first = process_message(raw, redis_client, pg_conn)
    second = process_message(raw, redis_client, pg_conn)

    assert first.outcome == "inserted"
    assert second.outcome == "deduped_by_cache"
    assert count_events(pg_conn) == 1


def test_malformed_json_is_poison():
    with pytest.raises(PoisonMessageError):
        process_message(b"not valid json", None, None)


def test_schema_violation_is_poison():
    bad_payload = json.dumps({"event_id": "abc", "environment": "not-a-real-env"}).encode()
    with pytest.raises(PoisonMessageError):
        process_message(bad_payload, None, None)


def test_simulated_transient_failure_raises_retryable_error(redis_client, pg_conn):
    with pytest.raises(TransientProcessingError):
        process_message(
            make_raw_event(str(uuid.uuid4())),
            redis_client,
            pg_conn,
            simulate_transient_failure=True,
        )
