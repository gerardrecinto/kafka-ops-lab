"""The runnable consumer. Wires: fetch -> process (pipeline.py) ->
route on failure (retry topic or DLQ) -> commit -> backpressure check.

Offsets are committed manually, after the message has been terminally
handled one way or another (inserted, deduped, routed to retry, or routed
to DLQ) — never before. A crash between processing and commit means the
message is redelivered, which `process_message`'s idempotency handles
correctly (see kafka_ops_lab/consumer/idempotency.py and
sink/postgres_sink.py's ON CONFLICT).
"""

import time

import psycopg
import redis
from kafka import KafkaConsumer

from kafka_ops_lab.consumer.backpressure import BackpressureController
from kafka_ops_lab.consumer.pipeline import PoisonMessageError, TransientProcessingError, process_message
from kafka_ops_lab.producer.deploy_producer import make_producer
from kafka_ops_lab.topics import DLQ_TOPIC, MAIN_TOPIC, MAX_RETRIES, RETRY_TOPIC


def _retry_count(headers: list[tuple[str, bytes]]) -> int:
    for key, value in headers or []:
        if key == "retry_count":
            return int(value.decode("utf-8"))
    return 0


def run_consumer_loop(
    bootstrap_servers: str,
    group_id: str,
    redis_client: redis.Redis,
    pg_conn: psycopg.Connection,
    topic: str = MAIN_TOPIC,
    max_messages: int | None = None,
    backpressure: BackpressureController | None = None,
) -> dict[str, int]:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    dlq_producer = make_producer(bootstrap_servers)
    backpressure = backpressure or BackpressureController()

    outcomes = {"inserted": 0, "deduped_by_cache": 0, "deduped_by_db": 0, "retried": 0, "dlq": 0}
    processed = 0

    try:
        for message in consumer:
            start = time.perf_counter()
            try:
                result = process_message(message.value, redis_client, pg_conn)
                outcomes[result.outcome] += 1
            except PoisonMessageError as exc:
                dlq_producer.send(DLQ_TOPIC, key=message.key, value=message.value, headers=[("error", str(exc).encode())])
                dlq_producer.flush()
                outcomes["dlq"] += 1
            except TransientProcessingError:
                retry_count = _retry_count(message.headers) + 1
                target = RETRY_TOPIC if retry_count <= MAX_RETRIES else DLQ_TOPIC
                dlq_producer.send(
                    target,
                    key=message.key,
                    value=message.value,
                    headers=[("retry_count", str(retry_count).encode())],
                )
                dlq_producer.flush()
                outcomes["retried" if target == RETRY_TOPIC else "dlq"] += 1

            consumer.commit()

            latency_ms = (time.perf_counter() - start) * 1000
            if backpressure.record_latency(latency_ms):
                consumer.pause(*consumer.assignment())
            else:
                consumer.resume(*consumer.assignment())

            processed += 1
            if max_messages is not None and processed >= max_messages:
                break
    finally:
        consumer.close()
        dlq_producer.close()

    return outcomes
