"""Black-box conformance suite for the annotation stub (section 5).

Speaks plain HTTP to STUB_URL (default http://localhost:8080); imports no pipeline code.
Run with:  ./intake up  &&  python3 -m pytest tests/test_stub_conformance.py -v
"""
import base64
import json
import os
import threading
import time
import urllib.error
import urllib.request

try:
    import pytest
except ModuleNotFoundError:
    # The suite remains runnable on a clean Python installation. When pytest is
    # installed, its normal fixture behavior is used instead.
    class _PytestFallback:
        @staticmethod
        def fixture(**_kwargs):
            return lambda fn: fn

    pytest = _PytestFallback()

STUB_URL = os.environ.get("STUB_URL", "http://localhost:8080").rstrip("/")


def call(method, path, body=None, timeout=20):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        STUB_URL + path, data=data, method=method,
        headers={"content-type": "application/json"} if data is not None else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null")


def annotate(payload, timeout=20):
    return call("POST", "/v1/annotate", payload, timeout)


def reset(**patch):
    status, body = call("POST", "/v1/reset", patch)
    assert status == 200, body
    return body


def stats():
    status, body = call("GET", "/v1/stats")
    assert status == 200
    return body


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


@pytest.fixture(autouse=True)
def fresh():
    reset(latency_mode="fixed", latency_ms=20, failure_every_n=7, failure_status=500, in_flight_capacity=2)
    yield


def test_healthz():
    assert call("GET", "/healthz")[0] == 200


def test_reset_clears_counters():
    annotate({"content_b64": b64(b"x")})
    assert stats()["billed_calls"] == 1
    reset()
    s = stats()
    assert s["billed_calls"] == 0 and s["server_error_calls"] == 0 and s["over_capacity_calls"] == 0
    assert s["max_in_flight"] == 0


def test_invalid_request_is_not_billed():
    for body in ({"content_b64": b64(b"a"), "tenant": "t"}, {"tenant": "t"}, {}):
        status, payload = annotate(body)
        assert status == 400 and payload["error"]["code"] == "invalid_request", body
    assert stats()["billed_calls"] == 0  # EXT-REQ-4: protocol errors are not billed


def test_annotation_includes_sha256_and_is_stable():
    import hashlib

    content = b"hello annotation"
    status, first = annotate({"content_b64": b64(content)})
    assert status == 200
    assert first["annotation"]["sha256"] == hashlib.sha256(content).hexdigest()
    status, second = annotate({"content_b64": b64(content)})
    assert status == 200
    assert second["annotation"] == first["annotation"]  # EXT-REQ-5
    assert stats()["billed_calls"] == 2  # repeat is billed again, no idempotency key


def test_failure_schedule_every_nth_billed_call():
    reset(latency_mode="fixed", latency_ms=5, failure_every_n=3, in_flight_capacity=1)
    statuses = [annotate({"content_b64": b64(bytes([i]))})[0] for i in range(9)]
    assert statuses == [200, 200, 500, 200, 200, 500, 200, 200, 500]  # EXT-REQ-2
    s = stats()
    assert s["billed_calls"] == 9 and s["server_error_calls"] == 3
    reset(failure_every_n=3)
    assert annotate({"content_b64": b64(b"a")})[0] == 200  # sequence restarted


def test_failures_disabled_when_zero():
    reset(latency_mode="fixed", latency_ms=5, failure_every_n=0, in_flight_capacity=1)
    assert [annotate({"content_b64": b64(bytes([i]))})[0] for i in range(8)] == [200] * 8
    assert stats()["server_error_calls"] == 0


def test_latency_is_configurable_and_repeatable():
    reset(latency_mode="fixed", latency_ms=300, failure_every_n=0, in_flight_capacity=1)
    t0 = time.time()
    assert annotate({"content_b64": b64(b"slow")})[0] == 200
    assert time.time() - t0 >= 0.28  # EXT-REQ-1

    def jitter_sequence():
        reset(latency_mode="jitter", latency_jitter_min_ms=10, latency_jitter_max_ms=200,
              latency_seed=4242, failure_every_n=0, in_flight_capacity=1)
        return [annotate({"content_b64": b64(bytes([i]))})[1]["meta"]["latency_ms"] for i in range(5)]

    assert jitter_sequence() == jitter_sequence()  # same config replays the same sequence


def test_over_capacity_is_distinct_and_billed():
    reset(latency_mode="fixed", latency_ms=600, failure_every_n=0, in_flight_capacity=2)
    results = []
    threads = [
        threading.Thread(target=lambda i=i: results.append(annotate({"content_b64": b64(bytes([i]))})))
        for i in range(4)
    ]
    for t in threads:
        t.start()
        time.sleep(0.05)
    for t in threads:
        t.join()
    codes = sorted(status for status, _ in results)
    assert codes == [200, 200, 429, 429], codes  # EXT-REQ-3
    for status, body in results:
        if status == 429:
            assert body["error"]["code"] == "over_capacity"
            assert "retry-after" not in json.dumps(body).lower()
    s = stats()
    assert s["over_capacity_calls"] == 2
    assert s["billed_calls"] == 4  # EXT-REQ-4: 429s are billed too
    assert s["max_in_flight"] == 2  # observed peak, never above the configured cap
    assert s["current_in_flight"] == 0


def test_server_error_holds_a_slot_for_its_full_duration():
    reset(latency_mode="fixed", latency_ms=500, failure_every_n=1, in_flight_capacity=1)
    slow = threading.Thread(target=lambda: annotate({"content_b64": b64(b"err")}))
    slow.start()
    time.sleep(0.15)
    assert stats()["current_in_flight"] == 1
    assert annotate({"content_b64": b64(b"blocked")})[0] == 429
    slow.join()
    assert stats()["server_error_calls"] == 1


def test_stats_fields_present():
    for k in ("billed_calls", "current_in_flight", "max_in_flight", "server_error_calls", "over_capacity_calls"):
        assert k in stats(), k  # EXT-REQ-6


def test_reset_rejects_unknown_config_field():
    assert call("POST", "/v1/reset", {"nope": 1})[0] == 400  # EXT-REQ-7 is a flat known-field patch


if __name__ == "__main__":
    failures = []
    for name, test in sorted(globals().items()):
        if name.startswith("test_") and callable(test):
            try:
                reset(latency_mode="fixed", latency_ms=20, failure_every_n=7, failure_status=500, in_flight_capacity=2)
                test()
                print(f"PASS {name}")
            except Exception as exc:
                failures.append((name, exc))
                print(f"FAIL {name}: {exc}")
    raise SystemExit(bool(failures))
