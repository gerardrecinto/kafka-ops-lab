# Troubleshooting

## Tests skip with "local Kafka/Postgres/Redis not reachable"

Run `docker compose up -d` first, and give Kafka a few seconds to finish
KRaft startup (`docker logs kafka-ops-lab-kafka-1` should show `Kafka
Server started`) before running tests immediately after `docker compose up`.

## `smoke_test.py` / `investigate_imbalance.py` get slower on repeated runs

By design, `MAIN_TOPIC` (`deploy-events`) accumulates messages across runs
— it's meant to model a real persistent topic, not an ephemeral test
fixture. Each smoke test run uses a fresh consumer group starting from
`earliest`, so it replays the entire topic history every time. This is
fine at lab scale (hundreds of messages) but would not scale as a
repeated CI smoke test against a long-lived topic — a real CI setup
would either use a dedicated short-retention topic per run (as
`tests/test_consumer_loop.py` and `tests/test_partition_imbalance.py` do
with fresh UUID-suffixed topic names) or seek to a specific offset instead
of `earliest`.

## `kafka-python-ng` vs `kafka-python`

This repo uses `kafka-python-ng` (a maintained fork), not the original
`kafka-python` package — the original has had long gaps between releases
and compatibility issues with newer Python versions. Same import name
(`import kafka`), so this is transparent to the code; only
`requirements.txt` names the fork explicitly.

## `docker compose up -d` fails to pull `apache/kafka:3.8.0`

Check `docker info` — the Docker daemon may not be running yet even
though the CLI is installed. On macOS: `open -a Docker`, wait for it to
report `docker info` successfully, then retry.

## Postgres port conflicts with another local Postgres

This repo's `docker-compose.yml` maps Postgres to host port **5433**, not
the default 5432 — deliberately, to avoid colliding with a system-level
Postgres. Redis maps to **6380**, not 6379, for the same reason (the
sibling `genai-gateway` project's Redis already uses 6379 locally).
