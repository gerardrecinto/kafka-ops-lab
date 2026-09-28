"""Latency-triggered backpressure: if the sink (Postgres write) starts
taking too long, pause fetching new records for a fixed number of poll
cycles rather than letting an unbounded number of in-flight messages pile
up client-side. Deliberately simple (a threshold + fixed cooldown, not a
PID controller) — the point is demonstrating the pause/resume mechanism
against a real measured signal, not building an adaptive controller.
"""


class BackpressureController:
    def __init__(self, latency_threshold_ms: float = 200.0, cooldown_batches: int = 5):
        self.latency_threshold_ms = latency_threshold_ms
        self.cooldown_batches = cooldown_batches
        self._paused = False
        self._cooldown_remaining = 0

    def record_latency(self, latency_ms: float) -> bool:
        """Call once per processed message. Returns True if the consumer
        should be paused right now (either just triggered, or still in its
        cooldown window)."""
        if self._paused:
            self._cooldown_remaining -= 1
            if self._cooldown_remaining <= 0:
                self._paused = False
            return self._paused

        if latency_ms > self.latency_threshold_ms:
            self._paused = True
            self._cooldown_remaining = self.cooldown_batches
            return True

        return False
