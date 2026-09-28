"""Consumer group lag, computed the same way `kafka-consumer-groups.sh
--describe` does: committed offset (from the admin client) vs. log-end
offset (from a throwaway consumer), per partition.
"""

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient


def get_consumer_group_lag(bootstrap_servers: str, group_id: str, topic: str) -> dict[int, int]:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="kafka-ops-lab-lag-check")
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, enable_auto_commit=False)
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return {}
        topic_partitions = [TopicPartition(topic, p) for p in sorted(partitions)]
        end_offsets = consumer.end_offsets(topic_partitions)

        committed = admin.list_consumer_group_offsets(group_id)
        lag = {}
        for tp in topic_partitions:
            committed_offset = committed.get(tp)
            committed_value = committed_offset.offset if committed_offset else 0
            lag[tp.partition] = max(0, end_offsets[tp] - committed_value)
        return lag
    finally:
        consumer.close()
        admin.close()


def total_lag(lag_by_partition: dict[int, int]) -> int:
    return sum(lag_by_partition.values())
