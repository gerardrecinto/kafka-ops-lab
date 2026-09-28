# kafka-ops-lab

A production-style Kafka event-processing system built to demonstrate
hands-on operational depth: partition design, idempotent consumers with a
Redis fast-path in front of a Postgres source of truth, retry/DLQ topics,
schema compatibility checking, lag monitoring, backpressure, and a
reproducible investigation of partition imbalance — all run against a
real local Kafka broker (KRaft mode), Postgres, and Redis via Docker, not
mocked.

**Honesty note**: I have hands-on Kafka experience from a real production
incident (diagnosing and fixing a consumer group's uneven partition
distribution, cutting broker CPU from ~80% to ~16%). This project is an
independently built portfolio piece that generalizes that experience into
a reusable, reproducible lab — it does not reuse or expose anything about
the original production system, and the specific numbers in this repo's
"Observed results" below are from this lab, not that incident.

## Architecture

```
producer (deploy_producer.py)
      |  keyed by "service" (high cardinality) or "environment" (low --
      |  see the imbalance investigation below for why that distinction matters)
      v
Kafka topic: deploy-events (6 partitions)
      |
      v
consumer group (consumer_loop.py)
      |
      +-- schema validation (schema/deploy_event.py)
      |        invalid --> deploy-events-dlq (poison, will never succeed on retry)
      |
      +-- Redis dedup fast-path (consumer/idempotency.py, bounded TTL)
      |        already seen --> skip Postgres write, commit offset
      |
      +-- Postgres upsert, ON CONFLICT DO NOTHING (sink/postgres_sink.py)
      |        source of truth for idempotency, unbounded (not TTL-limited)
      |
      +-- simulated transient failure --> deploy-events-retry (retry_count header)
      |        retry_count > MAX_RETRIES --> deploy-events-dlq
      |
      +-- backpressure controller (consumer/backpressure.py)
               high per-message latency --> consumer.pause() for a cooldown window
```

## Local quickstart

```bash
docker compose up -d          # Kafka (KRaft, single broker), Postgres, Redis
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install -e .

python -m kafka_ops_lab.admin              # create deploy-events / retry / dlq topics
python scripts/smoke_test.py               # produce, consume, verify end-to-end
```

Run tests (each integration test auto-skips if the relevant service isn't
reachable, via `tests/conftest.py` fixtures):

```bash
python -m pytest tests/ -v
```

**Observed**: 19/19 tests passing against a real local Kafka broker,
Postgres, and Redis (not mocked) — including the consumer-loop
end-to-end test and the partition-imbalance test, both of which produce
and consume real messages.

## Reproducible partition-imbalance investigation

`scripts/investigate_imbalance.py` produces 1,000 events to two fresh
6-partition topics — identical producer, identical event data, differing
only in partition key. Run it yourself: `python scripts/investigate_imbalance.py`.

**Observed** (this run; re-running will vary slightly since generation
uses `random.Random(seed=1)` per topic but partition assignment depends
on hashing, not the values themselves):

| Key strategy | Cardinality | Empty partitions | Imbalance (max above mean) |
|---|---|---|---|
| `service` (20 distinct values) | high | 0 / 6 | 79.4% |
| `environment` (3 distinct values) | low | 3 / 6 | 104.0% |

The headline number isn't the imbalance percentage — even the "good" key
isn't perfectly even, because 20 keys hashed across 6 partitions will
never split exactly evenly. The real finding is **empty partitions**: the
low-cardinality key structurally guarantees at least 3 of 6 partitions
never receive traffic, no matter how much data flows through, because
there are only 3 possible key values. That's a fundamentally different
failure mode from "somewhat uneven" — it means 50% of your consumer
group's parallelism is permanently wasted. This is the single most common
root cause of Kafka partition-imbalance incidents in practice: picking a
key field for correctness (ordering guarantees) without checking its
cardinality against the partition count.

## Idempotency: two layers, one source of truth

`consumer/idempotency.py` (Redis, TTL-bounded) is a fast-path that avoids
a Postgres round-trip for messages redelivered soon after they were first
processed — the common case during a consumer rebalance or a producer
retry. `sink/postgres_sink.py` (`ON CONFLICT (event_id) DO NOTHING`) is
the actual correctness guarantee, unbounded in time. If Redis were wiped
or unreachable, correctness wouldn't break — every message would just
cost one extra Postgres round-trip to discover it's already there. This
mirrors `release-risk-ml`'s and `genai-gateway`'s pattern of a fast
deterministic layer in front of the expensive/authoritative one.

## Repo layout

```
kafka_ops_lab/
  topics.py                    topic names, partition counts, retry limits
  admin.py                     idempotent topic creation
  schema/
    deploy_event.py            the event schema (pydantic)
    compatibility.py           generic backward-compatibility checker between schema versions
  producer/deploy_producer.py  synthetic event generator + good/bad key strategies
  consumer/
    idempotency.py             Redis fast-path dedup
    pipeline.py                pure process-one-message step (testable without Kafka)
    backpressure.py            latency-triggered pause/resume controller
    consumer_loop.py           the runnable consumer: fetch -> process -> retry/DLQ -> commit
  sink/postgres_sink.py        durable sink, upsert is the idempotency source of truth
  monitoring/
    lag.py                     consumer group lag, same method as kafka-consumer-groups.sh
    partition_stats.py         per-partition message counts, imbalance metric
docker-compose.yml             Kafka (KRaft), Postgres, Redis
scripts/
  investigate_imbalance.py     reproducible imbalance demonstration
  smoke_test.py                end-to-end: produce, consume, verify, check lag
tests/                          19 tests, real integration against local services
docs/                            runbooks, security, cost, troubleshooting, portfolio summary
```
