import uuid

import redis

from kafka_ops_lab.consumer.idempotency import claim_event


def test_first_claim_succeeds(redis_client):
    event_id = str(uuid.uuid4())
    assert claim_event(redis_client, event_id) is True


def test_second_claim_of_same_event_fails(redis_client):
    event_id = str(uuid.uuid4())
    assert claim_event(redis_client, event_id) is True
    assert claim_event(redis_client, event_id) is False


def test_different_events_claim_independently(redis_client):
    assert claim_event(redis_client, str(uuid.uuid4())) is True
    assert claim_event(redis_client, str(uuid.uuid4())) is True


def test_unreachable_redis_fails_open():
    unreachable_client = redis.Redis(host="localhost", port=1, socket_connect_timeout=1)
    assert claim_event(unreachable_client, str(uuid.uuid4())) is True
