"""Topic and partition design.

`deploy-events` gets 6 partitions: enough headroom for a consumer group of
up to 6 instances to each own at least one partition (a consumer group
larger than the partition count leaves members idle — see
docs/RUNBOOKS.md). Events are keyed by `service`, so all events for one
service land in the same partition and are processed in order relative to
each other; ordering across different services is not guaranteed or
needed here.

`deploy-events-retry` and `deploy-events-dlq` are separate topics, not
the same topic with a "retry" flag — this keeps retry/poison traffic from
ever competing for the same partition throughput as the main stream, and
lets a consumer group subscribe to the DLQ independently for inspection
without touching live traffic.
"""

MAIN_TOPIC = "deploy-events"
MAIN_TOPIC_PARTITIONS = 6

RETRY_TOPIC = "deploy-events-retry"
RETRY_TOPIC_PARTITIONS = 3
MAX_RETRIES = 3

DLQ_TOPIC = "deploy-events-dlq"
DLQ_TOPIC_PARTITIONS = 1

REPLICATION_FACTOR = 1  # single-broker local lab; production would use 3
