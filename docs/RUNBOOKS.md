# Runbooks

## Consumer group lag is climbing

1. Check total lag: `python -c "from kafka_ops_lab.monitoring.lag import *; print(total_lag(get_consumer_group_lag('localhost:9092', '<group_id>', 'deploy-events')))"`
2. Check per-partition lag with the same call minus `total_lag(...)` — a lag
   concentrated on one or two partitions (not spread evenly) points at
   partition imbalance (see the next runbook), not overall undercapacity.
3. If lag is spread evenly across all partitions, the consumer group is
   genuinely under-provisioned for the current throughput — add consumer
   instances, up to the partition count (`MAIN_TOPIC_PARTITIONS` in
   `topics.py`). Beyond that count, additional instances sit idle; see
   "Consumer group has more members than partitions" below.
4. Check `docs/MONITORING.md` for what `BackpressureController` does when
   per-message latency itself is the bottleneck (slow Postgres writes),
   as opposed to raw message volume — those need different fixes (scale
   Postgres/reduce write latency vs. scale consumers).

## Suspected partition imbalance

1. Run `python scripts/investigate_imbalance.py` conceptually — or, on
   the real topic, use `monitoring/partition_stats.py::get_partition_message_counts`
   against `deploy-events` directly to see the live per-partition counts.
2. Look for **empty or near-empty partitions**, not just "uneven" counts —
   an empty partition is the structural signal (a low-cardinality key),
   not a random fluctuation. `imbalance_pct_above_mean()` will also spike
   but the empty-partition count is the more legible root-cause signal
   (see README's investigation table: environment-keyed had 3/6 empty
   partitions; service-keyed had 0/6).
3. Root cause is almost always the producer's partition key choice, not
   the consumer side. Check `producer/deploy_producer.py::key_for()` (or
   the real production producer, if this were live) for what field is
   being hashed and its cardinality relative to partition count.
4. Fix: repartition by a higher-cardinality field, or accept the skew and
   size the partition count to the key cardinality instead (e.g., 3
   partitions if `environment` really is the correct ordering key).
   Changing partition count on an existing topic does NOT retroactively
   rebalance already-written data — new messages get the new partition
   count, old ones stay where they were written.

## Dead-letter topic (`deploy-events-dlq`) is growing

1. Read a sample of DLQ messages and their `error` header
   (`consumer_loop.py` sets `headers=[("error", str(exc).encode())]` for
   poison messages, `headers=[("retry_count", ...)]` for exhausted
   retries — the header tells you which).
2. **Schema validation failures** (poison, `PoisonMessageError`): almost
   always an upstream producer running an incompatible schema version.
   Check `schema/compatibility.py::check_backward_compatibility` against
   the currently deployed `DeployEvent` schema — if a producer added a
   new required field or removed one the consumer expects, that's the fix
   target (fix the producer or bump consumer compatibility), not
   something to patch around in the consumer.
3. **Exhausted retries** (`retry_count > MAX_RETRIES`, from
   `TransientProcessingError`): check whether the transient failure is
   still ongoing (e.g., Postgres was down for an extended window, not a
   brief blip) — `MAX_RETRIES` (`topics.py`) is tuned for transient
   blips, not sustained outages. A sustained outage needs the sink fixed
   first; replaying the DLQ before that just refills it.
4. Replaying a fixed DLQ message: re-produce its value to `deploy-events`
   (or a dedicated replay topic) once the root cause is fixed — there's no
   automated DLQ-replay tool in this repo; it's a manual, deliberate step
   by design (you don't want a bad schema silently reprocessing in a loop).

## Consumer group has more members than partitions

`deploy-events` has 6 partitions (`MAIN_TOPIC_PARTITIONS`). A 7th consumer
instance in the same group receives zero partitions and sits permanently
idle — Kafka's consumer group protocol assigns at most one partition
owner at a time, and extra members beyond the partition count get nothing.
This isn't a bug to fix in code; it's a capacity-planning constraint:
either increase partition count before scaling the consumer group further,
or stop scaling consumers past the current partition count.

## Rebalance is taking a long time / consumers appear stuck

`KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0` in `docker-compose.yml` is set
to 0 for this lab specifically so rebalances happen immediately for fast
iteration — production Kafka defaults this higher (commonly 3000ms) to
batch multiple near-simultaneous joins into one rebalance instead of
several. If rebalances in a real deployment take unexpectedly long, check
this setting isn't set unusually high, and check for consumers with slow
`poll()` loops exceeding `max.poll.interval.ms` — a consumer that's
silently stuck processing (e.g., blocked on a slow Postgres write with no
timeout) looks identical to a rebalance stall from outside.
