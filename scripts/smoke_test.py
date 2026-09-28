"""End-to-end smoke test: create topics, produce N real events, consume
them for real, verify every produced event_id lands in Postgres and lag
drops to zero. Safe to re-run — MAIN_TOPIC accumulates across runs by
design (it's meant to model a real persistent topic), so this checks
specific event_ids rather than absolute row counts. Requires
`docker compose up -d` to be running. Run:

    python scripts/smoke_test.py
"""

import os
import sys
import uuid

import psycopg
import redis

from kafka_ops_lab.admin import create_topics
from kafka_ops_lab.consumer.consumer_loop import run_consumer_loop
from kafka_ops_lab.monitoring.lag import get_consumer_group_lag, total_lag
from kafka_ops_lab.producer.deploy_producer import make_producer, produce_events
from kafka_ops_lab.sink.postgres_sink import ensure_schema
from kafka_ops_lab.topics import MAIN_TOPIC

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql://kafka_ops:kafka_ops@localhost:5433/kafka_ops_lab")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6380")
N_EVENTS = 25


def all_event_ids_present(conn: psycopg.Connection, event_ids: list[str]) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM deploy_events WHERE event_id = ANY(%s)", (event_ids,))
        return cur.fetchone()[0] == len(event_ids)


def main() -> None:
    print("== creating topics ==")
    create_topics(BOOTSTRAP)

    print(f"== producing {N_EVENTS} events to {MAIN_TOPIC} ==")
    producer = make_producer(BOOTSTRAP)
    events = produce_events(producer, MAIN_TOPIC, n_events=N_EVENTS, key_strategy="service", seed=int(uuid.uuid4()) % 10_000)
    producer.close()

    redis_client = redis.Redis.from_url(REDIS_URL)
    pg_conn = psycopg.connect(POSTGRES_DSN)
    ensure_schema(pg_conn)

    group_id = f"smoke-test-{uuid.uuid4()}"
    print(f"== consuming with fresh group {group_id} until caught up ==")
    outcomes = run_consumer_loop(BOOTSTRAP, group_id=group_id, redis_client=redis_client, pg_conn=pg_conn)
    print(f"outcomes: {outcomes}")

    event_ids = [event["event_id"] for event in events]
    present = all_event_ids_present(pg_conn, event_ids)
    print(f"all {len(event_ids)} produced event_ids present in Postgres: {present}")

    lag = get_consumer_group_lag(BOOTSTRAP, group_id, MAIN_TOPIC)
    print(f"lag by partition: {lag}, total: {total_lag(lag)}")

    if not present:
        print("FAIL: not all produced events were found in Postgres")
        sys.exit(1)
    if total_lag(lag) != 0:
        print(f"FAIL: expected 0 lag after full consumption, got {total_lag(lag)}")
        sys.exit(1)

    print("smoke test passed")


if __name__ == "__main__":
    main()
