# Cost notes

Everything in this project runs in local Docker containers
(`docker-compose.yml`) — no managed cloud services (MSK, RDS,
ElastiCache) were provisioned, so no cloud cost was incurred building or
testing it.

## Design decisions that would matter for cost at production scale

- **Separate retry and DLQ topics, not a shared one.** Keeps poison/retry
  traffic from competing for partition throughput with the main stream —
  in a managed Kafka service billed partly by partition count and
  throughput, isolating low-volume retry/DLQ traffic onto small,
  low-partition-count topics (3 and 1 partitions here, vs. 6 for the main
  topic) avoids over-provisioning them to match main-topic capacity.
- **Redis TTL-bounded dedup reduces Postgres load**, which matters more
  for RDS cost (often billed by instance size/IOPS) than Kafka cost — see
  README's "Idempotency: two layers" section. Fewer redundant writes means
  a smaller Postgres instance can handle the same event volume.
- **`REPLICATION_FACTOR = 1`** (`topics.py`) is a local-lab-only setting —
  it directly trades cost for durability (no replica means no protection
  against a single broker/disk failure). A real deployment needs at least
  3 for meaningful durability, which is closer to 3x the storage cost per
  topic for the same retention window.

## What's not modeled

No production Kafka cluster sizing, MSK/Confluent Cloud pricing
comparison, or throughput-to-instance-count sizing exercise is included
here — that would need real production traffic numbers this project
doesn't have. If asked in an interview: this project demonstrates the
architectural patterns; sizing a real cluster is a separate exercise that
depends on actual event volume and retention requirements.
