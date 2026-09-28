import uuid

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from kafka_ops_lab.consumer.consumer_loop import run_consumer_loop
from kafka_ops_lab.monitoring.lag import get_consumer_group_lag, total_lag
from kafka_ops_lab.producer.deploy_producer import make_producer, produce_events
from kafka_ops_lab.sink.postgres_sink import count_events

def _create_fresh_topic(bootstrap_servers: str) -> str:
    # Fresh topic name per test run — a fixed name would accumulate messages
    # from previous runs and make max_messages/lag assertions non-idempotent.
    topic = f"test-consumer-loop-{uuid.uuid4()}"
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="kafka-ops-lab-test-setup")
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=3, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()
    return topic


def test_consumer_loop_processes_valid_events_end_to_end(kafka_bootstrap, redis_client, pg_conn):
    topic = _create_fresh_topic(kafka_bootstrap)

    producer = make_producer(kafka_bootstrap)
    events = produce_events(producer, topic, n_events=5, key_strategy="service", seed=123)
    producer.close()

    group_id = f"test-group-{uuid.uuid4()}"
    outcomes = run_consumer_loop(
        kafka_bootstrap,
        group_id=group_id,
        redis_client=redis_client,
        pg_conn=pg_conn,
        topic=topic,
        max_messages=len(events),
    )

    assert outcomes["inserted"] == 5
    assert count_events(pg_conn) == 5

    lag = get_consumer_group_lag(kafka_bootstrap, group_id, topic)
    assert total_lag(lag) == 0
