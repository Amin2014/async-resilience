# async-resilience

A small, typed Python 3.12+ library for reliable, bounded asynchronous HTTP workloads.
One runtime dependency: [HTTPX](https://www.python-httpx.org/).

## 30-second quickstart

From this repository (the package is not yet published on PyPI):

```bash
python -m pip install -e .
python examples/local_demo.py
```

```python
import asyncio
from contextlib import aclosing
from async_resilience import Config, Request, ResilientClient


async def main():
    async with ResilientClient(Config(concurrency=32, per_host_concurrency=8)) as client:
        jobs = (Request("GET", f"https://example.org/items/{i}") for i in range(1000))
        async with aclosing(client.map(jobs, workers=32)) as results:
            async for result in results:
                if result.error is not None:
                    print(type(result.error).__name__)
                else:
                    print(result.response.status_code)
        print(client.statistics())


asyncio.run(main())
```

For a single request: `await client.request("GET", "https://example.org")`.
Responses are buffered `httpx.Response` objects; final HTTP errors are returned,
so call `response.raise_for_status()` if desired. Transport failures raise their
original exceptions. The full API is exported from `async_resilience` with PEP 561 typing.

## Architecture

`map` lazily pulls at most `workers` items, creates a fixed window of tasks, and
returns `Result` objects in completion order. Each request owns a cancellable
operation task. No worker daemon or unbounded queue runs in the background.

Each attempt acquires a per-origin semaphore followed by the global semaphore.
Origin means `(scheme, hostname, effective port)`. This order keeps one congested
origin from occupying all global permits. One shared HTTPX connection pool is
limited to global concurrency. Permits are released before retry sleeps.

A per-origin circuit breaker counts consecutive transient attempt failures:

- **CLOSED:** normal traffic; a non-transient response resets the counter.
- **OPEN:** admissions fail with `CircuitOpenError` until `recovery_timeout` expires.
- **HALF_OPEN:** exactly one admitted probe runs. A successful/non-transient response
  closes the circuit; a transient failure reopens it. Cancellation releases the probe.

Epoch tokens prevent stale concurrent completions from healing a newly opened circuit.
State updates run without suspension on one asyncio event loop. Inspect an origin
with `client.circuit_state(url)`; time alone does not mutate its observed state.
An open circuit interrupts a retry sequence and may replace the preceding transient
HTTP response with `CircuitOpenError`.

## Retry and timeout policy

Defaults retry GET, HEAD, OPTIONS, TRACE, PUT, and DELETE; POST and PATCH are never
retried by default, including on timeouts and HTTP 429. A server must implement
HTTP method semantics correctly for those defaults to be safe. Use `retry=False`
to disable replay, or `retry=True` only when your endpoint makes replay safe
(for example, using a server-enforced idempotency key). Bodies must be bytes or
strings; upload streams are intentionally unsupported.

Transient responses: **429, 500, 502, 503, 504**. Transient exceptions: HTTPX timeout,
network and remote protocol errors, plus the per-attempt wall-clock timeout.
Other statuses/errors are not retried. TLS/connect failures may be classified as
network errors by HTTPX and exhaust the same bounded retry budget.
`max_retries=3` means at most four attempts. Breaker thresholds count attempts,
not logical requests; HTTP 429 participates in that counter.

Backoff uses full jitter sampled uniformly from
`[0, min(backoff_cap, backoff_base * 2**attempt)]` (exponent saturates at 30).
Valid `Retry-After` delta-seconds and HTTP dates set a minimum delay for transient
responses, including 429 and 503. Past dates become zero; malformed values fall
back to jitter. A valid server delay is never shortened by the backoff cap.
Cancellation can interrupt even a long Retry-After wait. No retry sleep occurs
after the final attempt. This is per-request handling, not a shared host rate limiter.

`timeout` bounds the entire network attempt, including response reading; HTTPX
also applies it to individual network operations. Semaphore waits and backoff
are outside that deadline. To bound a whole workflow, wrap it in
`asyncio.timeout(total_seconds)`.

## Resource bounds and limitations

- `map` accepts a synchronous iterable and retains O(workers) request/results tasks;
  inputs must be cheap to iterate. Huge lists passed by the caller remain huge.
  Use `contextlib.aclosing` when breaking early to drain remaining work immediately.
- Direct `request` calls create one operation task each. The caller must bound its
  own task creation, or use `map`; semaphores cannot bound an external `gather`.
- `max_hosts=128` caps the persistent origin registry. A new origin beyond the cap
  raises `HostLimitError`. Entries are not evicted, preserving breaker history.
- `max_response_bytes=8 MiB` caps each buffered wire body. The client requests
  `Accept-Encoding: identity` and rejects compressed responses with
  `ContentEncodingError`, avoiding decompression bombs. HTTPX/OS headers, transport
  chunks, caller-owned request bodies and temporary copies still consume memory;
  this is bounded buffering, not an exact process RSS guarantee. Custom transports
  must not allocate unlimited bodies internally.
- No redirect following, automatic cookies-based login, streaming downloads,
  proxy environment discovery (`trust_env=False`), distributed state, persistence,
  shared rate limiter, fairness guarantee, or HTTP/2 dependency.
- Use one client per event loop; it is not thread safe. Cancellation is cooperative:
  custom transports must honor cancellation for shutdown to finish.
- Response cookies are rejected to keep cookie storage bounded. Send an explicit
  Cookie header if needed; automatic cookie sessions are unsupported.

## Cancellation, shutdown, logging and statistics

`async with` closes the pool and cancels/drains active request operations on exit.
`await client.aclose(grace=5)` first stops admission, allows existing operations
five seconds to finish, then cancels and drains the remainder. Close is idempotent;
concurrent calls use the first grace value. If the closing caller is cancelled,
cleanup continues in a shielded task; await `aclose()` again before stopping the loop.
Request and batch cancellation propagate `asyncio.CancelledError` without retrying.

Configure standard Python logging for `async_resilience`, or inject a `logger=`.
DEBUG retry records include `attempt` and `delay`, never request URLs or credentials.
There are no global logging handlers or basicConfig calls in the package.

`client.statistics()` returns an independent dataclass snapshot:
`requests`, `attempts`, `retries`, `completed`, `errors`, `cancelled`, `in_flight`,
and `peak_in_flight`. Completed includes final HTTP error responses; errors counts
raised request exceptions, including breaker rejection. Invalid inputs and registry
capacity rejections occur before request accounting. Retries counts scheduled
retry waits, even if cancelled before the next attempt.

## Local demo

`python examples/local_demo.py` starts a real loopback HTTP server on an ephemeral
port and closes it afterward. It simulates normal traffic, two 503s, a 429 with
`Retry-After: 1`, and repeated timeouts. No external network or extra dependency.
Example output (completion order can vary):

```text
ok: 200
flaky: 200
slow: TimeoutError
rate-limit: 200
{"attempts": 9, "cancelled": 0, "completed": 3, "errors": 1, "in_flight": 0, "peak_in_flight": 2, "requests": 4, "retries": 5}
```

The demo raises the breaker threshold so independent failure examples can complete
on the same origin. Production tuning should reflect the upstream service's traffic.

## Development

```bash
python -m pip install -e '.[dev]'
pytest -q
ruff check .
ruff format --check .
mypy
python -m build
```

Tests cover bounded concurrency and lazy consumption, cancellation and shutdown,
retry safety and Retry-After parsing, transport and wall-clock timeouts, breaker
transitions and stale completions, and resource limits. CI runs Python 3.12–3.14.
See `VERIFICATION.md` for the checks actually run on this delivery.

Contributions should include a regression test for behavior changes, keep the public
API typed, and avoid adding runtime dependencies without a concrete need. MIT licensed.
