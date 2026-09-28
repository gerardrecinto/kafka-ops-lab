# Security notes

## What's implemented

- **Poison messages never crash the consumer.** A schema-invalid message
  raises `PoisonMessageError`, gets routed to `deploy-events-dlq`, and
  processing continues — malformed or malicious input can't take the
  whole pipeline down (`consumer/pipeline.py`, `consumer/consumer_loop.py`).
- **Offsets commit only after terminal handling.** A message is never
  silently dropped: it's inserted, deduped, or routed to retry/DLQ before
  the offset advances (`consumer_loop.py`).

## Known limitations (stated, not hidden)

- **No authentication on any local service.** Kafka (`PLAINTEXT` listener),
  Postgres (fixed password in `docker-compose.yml`), and Redis (no
  `requirepass`) are all wide open — correct for a local lab, not how any
  of these would be run in production. A real deployment needs SASL/TLS
  for Kafka, a real secrets-managed Postgres credential, and Redis AUTH.
- **No encryption in transit.** Same root cause — local Docker Compose
  lab, not a hardened deployment.
- **Schema validation is a security control, not just a correctness one.**
  `PoisonMessageError` on a malformed payload means the pipeline can't be
  driven into an inconsistent state by a producer bug or a malicious
  producer with write access to the topic — but there is no producer-side
  authorization here; anything with network access to the broker can
  write to `deploy-events`.
