"""Per-partition message counts and an imbalance metric, computed straight
from broker offsets — no separate tracking system needed for a lab-scale
investigation.
"""

from kafka import KafkaConsumer, TopicPartition


def get_partition_message_counts(bootstrap_servers: str, topic: str) -> dict[int, int]:
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, enable_auto_commit=False)
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return {}
        topic_partitions = [TopicPartition(topic, p) for p in sorted(partitions)]
        consumer.assign(topic_partitions)
        beginning = consumer.beginning_offsets(topic_partitions)
        end = consumer.end_offsets(topic_partitions)
        return {tp.partition: end[tp] - beginning[tp] for tp in topic_partitions}
    finally:
        consumer.close()


def imbalance_pct_above_mean(counts: dict[int, int]) -> float:
    """How far the busiest partition sits above the mean, as a percentage.
    0% would mean perfectly even distribution across all partitions."""
    values = list(counts.values())
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    if mean == 0:
        return 0.0
    return round((max(values) - mean) / mean * 100, 1)
