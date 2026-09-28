"""The pure processing step, independent of Kafka — takes a raw message
payload, validates it, dedups it, writes it. Kept separate from the Kafka
consume loop (consumer_loop.py) so it's testable without a broker.
"""

import json
from dataclasses import dataclass

import psycopg
import redis
from pydantic import ValidationError

from kafka_ops_lab.consumer.idempotency import claim_event
from kafka_ops_lab.schema.deploy_event import DeployEvent
from kafka_ops_lab.sink.postgres_sink import upsert_event


class PoisonMessageError(Exception):
    """Raised for a message that will NEVER succeed no matter how many
    times it's retried (schema validation failure) — goes straight to the
    DLQ, not the retry topic."""


class TransientProcessingError(Exception):
    """Raised for a failure that might succeed on retry (e.g., a simulated
    DB hiccup) — goes to the retry topic with an incremented retry count."""


@dataclass
class ProcessResult:
    outcome: str  # "inserted" | "deduped_by_cache" | "deduped_by_db"
    event_id: str


def process_message(
    raw_value: bytes,
    redis_client: redis.Redis,
    pg_conn: psycopg.Connection,
    simulate_transient_failure: bool = False,
) -> ProcessResult:
    try:
        payload = json.loads(raw_value)
        event = DeployEvent(**payload)
    except (json.JSONDecodeError, ValidationError) as exc:
        raise PoisonMessageError(str(exc)) from exc

    if simulate_transient_failure:
        raise TransientProcessingError(f"simulated transient failure for event_id={event.event_id}")

    if not claim_event(redis_client, event.event_id):
        return ProcessResult(outcome="deduped_by_cache", event_id=event.event_id)

    inserted = upsert_event(pg_conn, event.model_dump())
    return ProcessResult(
        outcome="inserted" if inserted else "deduped_by_db",
        event_id=event.event_id,
    )
