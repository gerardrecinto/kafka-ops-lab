# Portfolio summary

## What this is

A production-style Kafka event pipeline demonstrating partition design,
idempotent consumption, retry/DLQ handling, schema compatibility
checking, lag and imbalance monitoring, and backpressure — run against a
real local Kafka broker, not mocked or simulated in-process.

I do have real production Kafka experience (diagnosing and fixing a
consumer group's uneven partition distribution, cutting broker CPU from
roughly 80% to 16%). This project is an independently built lab that
generalizes that experience into a reproducible demonstration — the
specific data and numbers here are from this lab, not a re-creation of
the original incident.

## Resume bullets (accurate language only)

- Built a Kafka event-processing pipeline with idempotent consumption
  (Redis fast-path over a Postgres source of truth), topic-based
  retry/DLQ handling, and schema backward-compatibility checking.
- Implemented a reproducible investigation demonstrating that partition
  key cardinality, not consumer throughput, is what causes uneven
  partition distribution — verified with real produced/consumed messages
  against a local Kafka broker.
- Implemented consumer group lag monitoring using the same offset-based
  method as `kafka-consumer-groups.sh --describe`.
- Implemented latency-triggered backpressure (pause/resume) as a
  documented, unit-tested mechanism.

**Not used, and why**: "reduced production lag by [X]%," "handled [N]
events/day in production" — none of these are true for this lab; all
numbers in this repo are from local runs against synthetic data, labeled
as such in the README.

## Interview talking points

**"Walk me through the imbalance investigation."**
Same producer, same event data, same partition count, one variable
changed: the partition key. Keying on `environment` (3 possible values)
against 6 partitions structurally guarantees half the partitions stay
empty — observed 3 of 6 empty, vs. 0 of 6 with a higher-cardinality key.
The interesting part isn't the imbalance percentage (even the good key
isn't perfectly even); it's that empty partitions are a categorically
different failure than "somewhat uneven," because they represent
permanently wasted consumer-group parallelism, not statistical noise.

**"How do you know Redis and Postgres don't disagree about what's been
processed?"**
They can't disagree in a way that breaks correctness, by construction:
Postgres's `ON CONFLICT DO NOTHING` on `event_id` is the actual
correctness boundary, unbounded in time. Redis is a TTL-bounded
optimization in front of it — if Redis said "not seen" incorrectly (e.g.,
after a TTL expiry or a Redis restart wiping state), the worst outcome is
one extra Postgres round-trip that finds the row already there and no-ops.
I verified this explicitly: `claim_event()` fails open (returns "proceed")
if Redis itself is unreachable, tested in
`tests/test_idempotency.py::test_unreachable_redis_fails_open`.

**"What would you build next if this were going to production?"**
Real metrics export (Prometheus/CloudWatch) instead of on-demand function
calls for lag and partition stats — see `docs/MONITORING.md`'s "Proposed"
section, stated as a named gap, not something I'm claiming already exists.

**"Have you run this at production scale?"**
No — this is a local Docker lab (single-broker KRaft Kafka, local
Postgres and Redis), not a sized production cluster. The patterns
(idempotency, retry/DLQ, schema compatibility, lag monitoring) generalize;
the actual capacity numbers here do not.

## Claims this project supports

- Hands-on Kafka topic/partition design and a working idempotent
  consumer group, verified against a real broker
- A reproducible, documented investigation into partition imbalance root
  cause with real observed numbers
- Working retry/DLQ topic routing and schema backward-compatibility
  checking (both unit and integration tested)
- Consumer group lag monitoring verified to reach exactly zero after full
  consumption

## Claims this project does NOT support

- Any production-scale deployment or real traffic volume
- Multi-broker replication or failover testing (single broker,
  `REPLICATION_FACTOR = 1`, stated directly in `docs/COST_NOTES.md`)
- Live metrics/alerting infrastructure (on-demand checks only)
