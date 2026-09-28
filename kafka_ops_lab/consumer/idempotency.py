"""Redis-backed fast-path dedup, bounded by a TTL window — not the
correctness guarantee (Postgres's `ON CONFLICT` is), but an optimization
that skips a DB round-trip for the common case: a message redelivered
shortly after a consumer rebalance or a producer retry. Postgres remains
authoritative outside the TTL window, which is the clear operational
benefit Redis provides here rather than being used as a second source of
truth.
"""

import redis

DEFAULT_TTL_SECONDS = 3600


def claim_event(client: redis.Redis, event_id: str, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> bool:
    """Returns True if this call claimed the event (i.e., it looks new
    within the TTL window) and processing should continue. Returns False
    if another call already claimed it recently — processing should skip
    straight to committing the offset without touching Postgres.

    Fails open (returns True) if Redis itself is unreachable: this is a
    performance fast-path, not the correctness boundary, so a Redis outage
    should degrade to "every message does a Postgres round-trip," not
    crash the consumer loop.
    """
    try:
        return bool(client.set(f"dedup:{event_id}", "1", nx=True, ex=ttl_seconds))
    except redis.exceptions.RedisError:
        return True
