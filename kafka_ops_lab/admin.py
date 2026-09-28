from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from kafka_ops_lab.topics import (
    DLQ_TOPIC,
    DLQ_TOPIC_PARTITIONS,
    MAIN_TOPIC,
    MAIN_TOPIC_PARTITIONS,
    REPLICATION_FACTOR,
    RETRY_TOPIC,
    RETRY_TOPIC_PARTITIONS,
)


def create_topics(bootstrap_servers: str) -> list[str]:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="kafka-ops-lab-admin")
    topics = [
        NewTopic(name=MAIN_TOPIC, num_partitions=MAIN_TOPIC_PARTITIONS, replication_factor=REPLICATION_FACTOR),
        NewTopic(name=RETRY_TOPIC, num_partitions=RETRY_TOPIC_PARTITIONS, replication_factor=REPLICATION_FACTOR),
        NewTopic(name=DLQ_TOPIC, num_partitions=DLQ_TOPIC_PARTITIONS, replication_factor=REPLICATION_FACTOR),
    ]
    created = []
    try:
        admin.create_topics(new_topics=topics, validate_only=False)
        created = [t.name for t in topics]
    except TopicAlreadyExistsError:
        # create_topics fails the whole batch on first collision; fall back
        # to creating one at a time so re-running this is idempotent.
        for topic in topics:
            try:
                admin.create_topics(new_topics=[topic], validate_only=False)
                created.append(topic.name)
            except TopicAlreadyExistsError:
                pass
    finally:
        admin.close()
    return created


if __name__ == "__main__":
    import os

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    result = create_topics(bootstrap)
    print(f"created or already present: {[MAIN_TOPIC, RETRY_TOPIC, DLQ_TOPIC]}")
    print(f"newly created this run: {result}")
