"""Synthetic deploy-event producer, with two selectable partition-key
strategies used to demonstrate good vs. bad key choice:

- "service": keys on service name (high cardinality — ~20 distinct
  services in generate_events). Spreads across all partitions.
- "environment": keys on environment (only 3 possible values: dev,
  staging, prod). This is a real, common mistake — the key is a low-
  cardinality field, so no matter how many partitions the topic has,
  at most 3 will ever receive traffic. This is the deliberately bad
  strategy used in scripts/investigate_imbalance.py.
"""

import json
import random
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

SERVICES = [f"service-{i:02d}" for i in range(20)]
ENVIRONMENTS = ["dev", "staging", "prod"]
STATUSES = ["succeeded", "succeeded", "succeeded", "failed", "rolled_back"]  # weighted toward success


def generate_event(rng: random.Random) -> dict:
    return {
        "schema_version": "1.1",
        "event_id": str(uuid.uuid4()),
        "service": rng.choice(SERVICES),
        "environment": rng.choice(ENVIRONMENTS),
        "status": rng.choice(STATUSES),
        "released_by": f"engineer-{rng.randint(1, 15)}",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(rng.uniform(30, 600), 2),
    }


def make_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
    )


def key_for(event: dict, key_strategy: str) -> str:
    if key_strategy == "service":
        return event["service"]
    if key_strategy == "environment":
        return event["environment"]
    raise ValueError(f"unknown key_strategy: {key_strategy}")


def produce_events(
    producer: KafkaProducer,
    topic: str,
    n_events: int,
    key_strategy: str,
    seed: int = 42,
) -> list[dict]:
    rng = random.Random(seed)
    events = [generate_event(rng) for _ in range(n_events)]
    for event in events:
        producer.send(topic, key=key_for(event, key_strategy), value=event)
    producer.flush()
    return events
