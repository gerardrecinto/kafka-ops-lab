"""A reproducible demonstration that partition key choice, not consumer
throughput, is what causes uneven partition distribution. Same producer,
same topic shape, two different keys.
"""

import uuid

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from kafka_ops_lab.monitoring.partition_stats import get_partition_message_counts, imbalance_pct_above_mean
from kafka_ops_lab.producer.deploy_producer import make_producer, produce_events


def _create_topic(bootstrap_servers: str, partitions: int) -> str:
    topic = f"test-imbalance-{uuid.uuid4()}"
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="kafka-ops-lab-test-setup")
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()
    return topic


def test_low_cardinality_key_causes_measurable_imbalance(kafka_bootstrap):
    good_topic = _create_topic(kafka_bootstrap, partitions=6)
    bad_topic = _create_topic(kafka_bootstrap, partitions=6)

    producer = make_producer(kafka_bootstrap)
    produce_events(producer, good_topic, n_events=600, key_strategy="service", seed=1)
    produce_events(producer, bad_topic, n_events=600, key_strategy="environment", seed=1)
    producer.close()

    good_counts = get_partition_message_counts(kafka_bootstrap, good_topic)
    bad_counts = get_partition_message_counts(kafka_bootstrap, bad_topic)

    good_imbalance = imbalance_pct_above_mean(good_counts)
    bad_imbalance = imbalance_pct_above_mean(bad_counts)

    # environment has only 3 distinct values against 6 partitions, so at
    # least 3 partitions must be completely empty (0 messages) -- a much
    # larger deviation from the mean than 20 well-spread service keys.
    empty_partitions_bad = sum(1 for count in bad_counts.values() if count == 0)
    assert empty_partitions_bad >= 3
    assert bad_imbalance > good_imbalance
