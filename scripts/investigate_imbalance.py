"""Reproducible partition-imbalance investigation.

Produces the same number of events to two fresh topics with the same
partition count, differing only in partition key choice, then reports the
resulting distribution. Run:

    python scripts/investigate_imbalance.py
"""

import os
import uuid

from kafka.admin import KafkaAdminClient, NewTopic

from kafka_ops_lab.monitoring.partition_stats import get_partition_message_counts, imbalance_pct_above_mean
from kafka_ops_lab.producer.deploy_producer import make_producer, produce_events

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
N_EVENTS = 1000
PARTITIONS = 6


def create_topic(name: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="investigate-imbalance")
    admin.create_topics([NewTopic(name=name, num_partitions=PARTITIONS, replication_factor=1)])
    admin.close()


def report(label: str, counts: dict[int, int]) -> None:
    print(f"\n{label}")
    for partition in sorted(counts):
        bar = "#" * (counts[partition] // 5)
        print(f"  partition {partition}: {counts[partition]:4d}  {bar}")
    print(f"  imbalance (max above mean): {imbalance_pct_above_mean(counts)}%")
    print(f"  empty partitions: {sum(1 for c in counts.values() if c == 0)} / {len(counts)}")


def main() -> None:
    good_topic = f"investigate-good-key-{uuid.uuid4().hex[:8]}"
    bad_topic = f"investigate-bad-key-{uuid.uuid4().hex[:8]}"
    create_topic(good_topic)
    create_topic(bad_topic)

    producer = make_producer(BOOTSTRAP)
    produce_events(producer, good_topic, n_events=N_EVENTS, key_strategy="service", seed=1)
    produce_events(producer, bad_topic, n_events=N_EVENTS, key_strategy="environment", seed=1)
    producer.close()

    good_counts = get_partition_message_counts(BOOTSTRAP, good_topic)
    bad_counts = get_partition_message_counts(BOOTSTRAP, bad_topic)

    report(f"GOOD KEY (service, 20 distinct values) -- topic {good_topic}", good_counts)
    report(f"BAD KEY (environment, 3 distinct values) -- topic {bad_topic}", bad_counts)


if __name__ == "__main__":
    main()
