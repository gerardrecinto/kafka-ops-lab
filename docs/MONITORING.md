# Monitoring

## What's actually running (Observed)

- `monitoring/lag.py::get_consumer_group_lag` — computed the same way
  `kafka-consumer-groups.sh --describe` does (committed offset vs.
  log-end offset per partition). Verified in
  `tests/test_consumer_loop.py` (lag reaches exactly 0 after full
  consumption) and `scripts/smoke_test.py`.
- `monitoring/partition_stats.py::get_partition_message_counts` and
  `imbalance_pct_above_mean` — verified against real topics in
  `tests/test_partition_imbalance.py` and `scripts/investigate_imbalance.py`.
- `consumer/backpressure.py::BackpressureController` — a per-message
  latency threshold with a fixed cooldown, wired into `consumer_loop.py`
  to call `consumer.pause()`/`resume()`. Unit-tested
  (`tests/test_backpressure.py`); not exercised against genuinely slow
  Postgres in this lab (Postgres here is fast and local — there's no
  realistic way to trigger the 200ms default threshold without
  artificially injecting latency, which none of the current scripts do).

## What's designed but not wired to alerting (Proposed)

This lab has no metrics exporter or alerting stack (no Prometheus,
CloudWatch, or Grafana wired in) — everything above is computed on demand
via a Python function call, not continuously scraped. A production
version would:

- Export `get_consumer_group_lag` results as a Prometheus gauge on a
  schedule (e.g., every 15s), alerting above a lag threshold tuned to
  the topic's actual throughput.
- Export per-partition message-rate deltas to catch imbalance emerging
  live, not just via an on-demand investigation script.
- Track `BackpressureController` pause events as a counter — frequent
  pausing is itself a signal the sink needs scaling, independent of
  whether lag has visibly climbed yet.

## Why Redis dedup doesn't need its own monitoring here

`consumer/idempotency.py`'s TTL-bounded dedup is a performance
optimization, not a correctness boundary (`sink/postgres_sink.py`'s
`ON CONFLICT` is that boundary) — see the README's "Idempotency: two
layers" section. That's a deliberate design property, not an oversight:
if Redis were down, `get_cache_backend`-style fallback isn't even needed
here, since `idempotency.claim_event` failing would raise from the Redis
client directly and surface as a normal consumer error, not a silent
correctness gap.
