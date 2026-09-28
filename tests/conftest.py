import psycopg
import pytest
import redis

from kafka_ops_lab.sink.postgres_sink import ensure_schema

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
POSTGRES_DSN = "postgresql://kafka_ops:kafka_ops@localhost:5433/kafka_ops_lab"
REDIS_URL = "redis://localhost:6380"


@pytest.fixture
def redis_client():
    client = redis.Redis.from_url(REDIS_URL)
    try:
        client.ping()
    except redis.exceptions.ConnectionError:
        pytest.skip(f"local Redis not reachable at {REDIS_URL} (docker compose up -d redis)")
    yield client
    client.close()


@pytest.fixture
def pg_conn():
    try:
        conn = psycopg.connect(POSTGRES_DSN)
    except psycopg.OperationalError:
        pytest.skip(f"local Postgres not reachable at {POSTGRES_DSN} (docker compose up -d postgres)")
    ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE deploy_events;")
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def kafka_bootstrap():
    from kafka import KafkaAdminClient

    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, client_id="kafka-ops-lab-test-check")
        admin.close()
    except Exception:
        pytest.skip(f"local Kafka not reachable at {KAFKA_BOOTSTRAP_SERVERS} (docker compose up -d kafka)")
    return KAFKA_BOOTSTRAP_SERVERS
