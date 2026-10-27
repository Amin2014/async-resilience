import asyncio
from collections import Counter
from contextlib import aclosing
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest

from async_resilience import (
    CircuitOpenError,
    CircuitState,
    Config,
    HostLimitError,
    Request,
    ResilientClient,
    ResponseTooLargeError,
)
from async_resilience._breaker import Breaker
from async_resilience._client import retry_after


def config(**kwargs):
    return Config(backoff_base=0.001, backoff_cap=0.002, **kwargs)


def response(status=200, **kwargs):
    # Streaming transport responses exercise the same body-reading path as sockets.
    return httpx.Response(status, stream=BytesStream(kwargs.pop("body", b"ok")), **kwargs)


class BytesStream(httpx.AsyncByteStream):
    def __init__(self, body):
        self.body = body
        self.closed = False

    async def __aiter__(self):
        yield self.body

    async def aclose(self):
        self.closed = True


async def test_global_and_host_concurrency():
    active = Counter()
    peaks = Counter()

    async def handler(req):
        active[req.url.host] += 1
        active["all"] += 1
        for key, value in active.items():
            peaks[key] = max(peaks[key], value)
        await asyncio.sleep(0.005)
        active[req.url.host] -= 1
        active["all"] -= 1
        return response()

    async with ResilientClient(
        config(concurrency=3, per_host_concurrency=2), transport=httpx.MockTransport(handler)
    ) as client:
        results = [
            r
            async for r in client.map(
                (Request("GET", f"https://host{i % 2}.test/{i}") for i in range(24)), workers=8
            )
        ]
        assert all(r.response.status_code == 200 for r in results)
        assert peaks["all"] == 3
        assert peaks["host0.test"] == peaks["host1.test"] == 2
        assert client.statistics().in_flight == 0


@pytest.mark.parametrize(
    "method,retry,calls",
    [
        ("GET", None, 3),
        ("PUT", None, 3),
        ("DELETE", None, 3),
        ("POST", None, 1),
        ("PATCH", None, 1),
        ("POST", True, 3),
        ("GET", False, 1),
    ],
)
async def test_retry_safety(method, retry, calls):
    seen = []

    async def handler(req):
        seen.append(await req.aread())
        return response(503 if len(seen) < 3 else 200)

    async with ResilientClient(config(), transport=httpx.MockTransport(handler)) as client:
        result = await client.request(method, "https://test/a", retry=retry, content=b"payload")
        assert len(seen) == calls
        assert seen == [b"payload"] * calls
        assert result.status_code == (200 if calls == 3 else 503)
        assert client.statistics().retries == calls - 1


async def test_retry_after_and_jitter(monkeypatch):
    delays = []
    statuses = iter([429, 503, 200])

    async def sleep(delay):
        delays.append(delay)

    monkeypatch.setattr("async_resilience._client.asyncio.sleep", sleep)
    monkeypatch.setattr("async_resilience._client.random.uniform", lambda a, b: b)

    async def handler(req):
        return response(next(statuses), headers={"Retry-After": "2"})

    async with ResilientClient(config(), transport=httpx.MockTransport(handler)) as client:
        assert (await client.request("GET", "https://test")).status_code == 200
    assert delays == [2.0, 2.0]


def test_retry_after_parser():
    assert retry_after("12") == 12
    assert retry_after("0") == 0
    assert retry_after("-5") is None
    assert retry_after("nan") is None
    assert retry_after("garbage") is None
    assert retry_after("9" * 500) is None
    assert retry_after(format_datetime(datetime.now(UTC) - timedelta(days=1))) == 0
    future = format_datetime(datetime.now(UTC) + timedelta(seconds=20))
    assert 18 <= retry_after(future) <= 20


async def test_timeout_and_transport_retry():
    calls = 0

    async def handler(req):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("connection reset")
        await asyncio.sleep(0.2)
        return response()

    async with ResilientClient(
        config(timeout=0.01, max_retries=2), transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(TimeoutError):
            await client.request("GET", "https://test")
        assert calls == 3
        assert client.statistics().errors == 1
        assert client.statistics().in_flight == 0


async def test_httpx_timeout_retries():
    calls = 0

    async def handler(req):
        nonlocal calls
        calls += 1
        if calls < 2:
            raise httpx.ReadTimeout("slow", request=req)
        return response()

    async with ResilientClient(config(), transport=httpx.MockTransport(handler)) as client:
        assert (await client.request("GET", "https://test")).status_code == 200
        assert calls == 2


async def test_cancellation_releases_permits():
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(req):
        started.set()
        await release.wait()
        return response()

    async with ResilientClient(
        config(concurrency=1, per_host_concurrency=1), transport=httpx.MockTransport(handler)
    ) as client:
        task = asyncio.create_task(client.request("GET", "https://test"))
        await started.wait()
        queued = asyncio.create_task(client.request("GET", "https://test"))
        await asyncio.sleep(0)
        queued.cancel()
        task.cancel()
        for item in [queued, task]:
            with pytest.raises(asyncio.CancelledError):
                await item
        release.set()
        assert (await client.request("GET", "https://test")).status_code == 200
        assert client.statistics().cancelled == 2


async def test_circuit_transitions_and_probe_exclusion():
    clock = [0.0]
    breaker = Breaker(2, 10)
    for _ in range(2):
        token = breaker.admit(clock[0])
        breaker.finish(token, True, clock[0])
    assert breaker.state == CircuitState.OPEN
    with pytest.raises(CircuitOpenError):
        breaker.admit(9)
    token = breaker.admit(10)
    assert breaker.state == CircuitState.HALF_OPEN
    with pytest.raises(CircuitOpenError):
        breaker.admit(10)
    breaker.finish(token, None, 10)  # Cancelled probe permits a replacement.
    token = breaker.admit(10)
    breaker.finish(token, True, 10)
    assert breaker.state == CircuitState.OPEN
    token = breaker.admit(20)
    breaker.finish(token, False, 20)
    assert breaker.state == CircuitState.CLOSED


def test_stale_completion_cannot_heal_open_breaker():
    breaker = Breaker(1, 10)
    token = breaker.admit(0)
    breaker.finish(token, True, 0)
    breaker.finish(token, False, 1)
    assert breaker.state == CircuitState.OPEN


async def test_open_circuit_fails_fast_and_is_per_origin():
    async def handler(req):
        return response(503 if req.url.host == "bad" else 200)

    async with ResilientClient(
        config(failure_threshold=1, max_retries=0), transport=httpx.MockTransport(handler)
    ) as client:
        assert (await client.request("GET", "https://bad")).status_code == 503
        with pytest.raises(CircuitOpenError):
            await client.request("GET", "https://bad")
        assert (await client.request("GET", "https://good")).status_code == 200
        assert client.circuit_state("https://bad") == CircuitState.OPEN
        assert client.statistics().attempts == 2


async def test_bounded_lazy_map_and_early_close():
    consumed = 0
    started = asyncio.Event()
    release = asyncio.Event()

    def source():
        nonlocal consumed
        for i in range(100_000):
            consumed += 1
            yield Request("GET", f"https://test/{i}")

    async def handler(req):
        started.set()
        await release.wait()
        return response()

    async with ResilientClient(config(), transport=httpx.MockTransport(handler)) as client:
        iterator = client.map(source(), workers=4)
        next_result = asyncio.create_task(anext(iterator))
        await started.wait()
        assert consumed == 4
        release.set()
        await next_result
        await iterator.aclose()
        assert client.statistics().in_flight == 0
        assert consumed == 4


async def test_map_cancellation_drains_children():
    started = asyncio.Event()

    async def handler(req):
        started.set()
        await asyncio.Event().wait()

    async with ResilientClient(config(), transport=httpx.MockTransport(handler)) as client:

        async def consume():
            async with aclosing(client.map([Request("GET", "https://test")] * 4)) as results:
                async for _ in results:
                    pass

        task = asyncio.create_task(consume())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.statistics().in_flight == 0


@pytest.mark.parametrize("grace,finishes", [(0, False), (0.2, True)])
async def test_shutdown(grace, finishes):
    started = asyncio.Event()

    async def handler(req):
        started.set()
        await asyncio.sleep(0.02)
        return response()

    client = ResilientClient(config(), transport=httpx.MockTransport(handler))
    task = asyncio.create_task(client.request("GET", "https://test"))
    await started.wait()
    await client.aclose(grace=grace)
    if finishes:
        assert (await task).status_code == 200
    else:
        with pytest.raises(asyncio.CancelledError):
            await task
    await client.aclose()
    with pytest.raises(RuntimeError):
        await client.request("GET", "https://test")


async def test_body_and_registry_limits():
    stream = BytesStream(b"too large")

    async def handler(req):
        return httpx.Response(200, stream=stream)

    async with ResilientClient(
        config(max_hosts=1, max_response_bytes=2), transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(ResponseTooLargeError):
            await client.request("GET", "https://one")
        assert stream.closed
        with pytest.raises(HostLimitError):
            await client.request("GET", "https://two")
        assert client.statistics().attempts == 1
        snapshot = client.statistics()
        snapshot.attempts = 99
        assert client.statistics().attempts == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"concurrency": 0},
        {"max_retries": -1},
        {"timeout": float("nan")},
        {"per_host_concurrency": True},
        {"max_retries": 1.5},
        {"recovery_timeout": 0},
    ],
)
def test_invalid_config(kwargs):
    with pytest.raises(ValueError):
        Config(**kwargs)


async def test_compression_is_rejected_and_cookies_are_not_retained():
    from async_resilience import ContentEncodingError

    async def handler(req):
        assert req.headers["Accept-Encoding"] == "identity"
        assert "cookie" not in req.headers
        if req.url.path == "/compressed":
            return response(headers={"Content-Encoding": "gzip"})
        return response(headers={"Set-Cookie": "session=secret; Path=/"})

    async with ResilientClient(config(), transport=httpx.MockTransport(handler)) as client:
        await client.request("GET", "https://test/cookie")
        await client.request("GET", "https://test/again")
        assert len(client._http.cookies) == 0
        with pytest.raises(ContentEncodingError):
            await client.request("GET", "https://test/compressed")


async def test_backoff_cancellation_and_permits(monkeypatch):
    sleeping = asyncio.Event()
    calls = 0

    async def handler(req):
        nonlocal calls
        calls += 1
        return response(429 if calls == 1 else 200, headers={"Retry-After": "3600"})

    async with ResilientClient(config(concurrency=1), transport=httpx.MockTransport(handler)) as c:
        original_sleep = asyncio.sleep

        async def sleep(delay):
            if delay >= 3600:
                sleeping.set()
                await asyncio.Event().wait()
            else:
                await original_sleep(delay)

        monkeypatch.setattr("async_resilience._client.asyncio.sleep", sleep)
        task = asyncio.create_task(c.request("GET", "https://test"))
        await sleeping.wait()
        assert (await c.request("GET", "https://other")).status_code == 200
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert calls == 2
        assert c.statistics().in_flight == 0


async def test_body_read_has_wall_clock_deadline():
    class SlowBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"first"
            await asyncio.sleep(1)
            yield b"last"

    async def handler(req):
        return httpx.Response(200, stream=SlowBody())

    async with ResilientClient(
        config(timeout=0.01, max_retries=0), transport=httpx.MockTransport(handler)
    ) as c:
        with pytest.raises(TimeoutError):
            await c.request("GET", "https://test")


async def test_shutdown_does_not_wait_for_callers_remaining_work():
    received = asyncio.Event()
    release = asyncio.Event()

    async def handler(req):
        return response()

    client = ResilientClient(config(), transport=httpx.MockTransport(handler))

    async def caller():
        await client.request("GET", "https://test")
        received.set()
        await release.wait()

    task = asyncio.create_task(caller())
    await received.wait()
    await client.aclose()
    assert not task.done()
    release.set()
    await task


async def test_half_open_probe_integration(monkeypatch):
    now = [0.0]
    monkeypatch.setattr(
        "async_resilience._client.time",
        type("Clock", (), {"monotonic": staticmethod(lambda: now[0])}),
    )
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def handler(req):
        nonlocal calls
        calls += 1
        if calls == 1:
            return response(503)
        started.set()
        await release.wait()
        return response()

    async with ResilientClient(
        config(failure_threshold=1, recovery_timeout=10, max_retries=0),
        transport=httpx.MockTransport(handler),
    ) as c:
        await c.request("GET", "https://test")
        now[0] = 10
        probe = asyncio.create_task(c.request("GET", "https://test"))
        await started.wait()
        assert c.circuit_state("https://test") == CircuitState.HALF_OPEN
        with pytest.raises(CircuitOpenError):
            await c.request("GET", "https://test")
        release.set()
        await probe
        assert c.circuit_state("https://test") == CircuitState.CLOSED


async def test_jitter_exponential_cap_and_nontransient_status(monkeypatch):
    ceilings = []
    sleeps = []

    def uniform(low, high):
        ceilings.append(high)
        return high / 2

    async def sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr("async_resilience._client.random.uniform", uniform)
    monkeypatch.setattr("async_resilience._client.asyncio.sleep", sleep)

    async def handler(req):
        return response(400 if req.url.path == "/bad" else 503)

    cfg = Config(backoff_base=1, backoff_cap=3, failure_threshold=100, max_retries=4)
    async with ResilientClient(cfg, transport=httpx.MockTransport(handler)) as c:
        assert (await c.request("GET", "https://test")).status_code == 503
        attempts = c.statistics().attempts
        assert (await c.request("GET", "https://test/bad")).status_code == 400
        assert c.statistics().attempts == attempts + 1
    assert ceilings == [1, 2, 3, 3]
    assert sleeps == [0.5, 1, 1.5, 1.5]
