from kafka_ops_lab.consumer.backpressure import BackpressureController


def test_low_latency_never_pauses():
    controller = BackpressureController(latency_threshold_ms=200)
    for _ in range(10):
        assert controller.record_latency(10.0) is False


def test_high_latency_triggers_pause():
    controller = BackpressureController(latency_threshold_ms=200, cooldown_batches=3)
    assert controller.record_latency(500.0) is True


def test_pause_holds_for_cooldown_then_releases():
    controller = BackpressureController(latency_threshold_ms=200, cooldown_batches=2)
    assert controller.record_latency(500.0) is True  # triggers, cooldown=2
    assert controller.record_latency(10.0) is True  # still paused, cooldown=1
    assert controller.record_latency(10.0) is False  # cooldown exhausted, released
