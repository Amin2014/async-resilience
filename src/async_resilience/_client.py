"""Small, asyncio-only resilience layer. No background scheduler or worker daemon."""

from __future__ import annotations

import asyncio
import logging
import math
import random
import time
from collections.abc import AsyncIterator, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from http.cookiejar import CookieJar, DefaultCookiePolicy
from types import TracebackType

import httpx

from ._breaker import Breaker, CircuitState

IDEMPOTENT = frozenset({"GET", "HEAD", "OPTIONS", "TRACE", "PUT", "DELETE"})
TRANSIENT = frozenset({429, 500, 502, 503, 504})
RETRY_ERRORS = (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError, TimeoutError)


@dataclass(frozen=True, slots=True)
class Config:
    concurrency: int = 32
    per_host_concurrency: int = 8
    timeout: float = 10.0
    max_retries: int = 3
    backoff_base: float = 0.25
    backoff_cap: float = 10.0
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    max_hosts: int = 128
    max_response_bytes: int = 8 * 1024 * 1024

    def __post_init__(self) -> None:
        for name in (
            "concurrency",
            "per_host_concurrency",
            "failure_threshold",
            "max_hosts",
            "max_response_bytes",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if isinstance(self.max_retries, bool) or not isinstance(self.max_retries, int):
            raise ValueError("max_retries must be an integer")
        if self.max_retries < 0:
            raise ValueError("max_retries must be nonnegative")
        for name in ("timeout", "backoff_base", "backoff_cap", "recovery_timeout"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")


@dataclass(frozen=True, slots=True)
class Request:
    method: str
    url: str
    headers: Mapping[str, str] | None = None
    content: bytes | str | None = None
    retry: bool | None = None


@dataclass(frozen=True, slots=True)
class Result:
    request: Request
    response: httpx.Response | None = None
    error: Exception | None = None


@dataclass(slots=True)
class Statistics:
    requests: int = 0
    attempts: int = 0
    retries: int = 0
    completed: int = 0
    errors: int = 0
    cancelled: int = 0
    in_flight: int = 0
    peak_in_flight: int = 0


class HostLimitError(RuntimeError):
    """The bounded origin registry is full; use a separate client for more origins."""


class ContentEncodingError(RuntimeError):
    """A server ignored identity encoding; compressed responses are unsupported."""


class ResponseTooLargeError(RuntimeError):
    """The response exceeded the configured wire-body limit."""


def retry_after(value: str | None) -> float | None:
    """Parse RFC delta-seconds or an HTTP date; invalid values are ignored."""
    if value is None:
        return None
    value = value.strip()
    if value.isascii() and value.isdigit():
        try:
            seconds = float(value)
        except ValueError:
            return None
        return seconds if math.isfinite(seconds) else None
    try:
        date = parsedate_to_datetime(value)
        if date.tzinfo is None:
            date = date.replace(tzinfo=UTC)
        return max(0.0, date.timestamp() - datetime.now(UTC).timestamp())
    except (ValueError, TypeError, OverflowError):
        return None


class ResilientClient:
    """Owns its HTTPX transport. Use on a single event loop and close when done."""

    def __init__(
        self,
        config: Config | None = None,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.config = config or Config()
        self._log = logger or logging.getLogger("async_resilience")
        self._http = httpx.AsyncClient(
            transport=transport,
            timeout=self.config.timeout,
            trust_env=False,
            cookies=CookieJar(policy=DefaultCookiePolicy(allowed_domains=[])),
            limits=httpx.Limits(
                max_connections=self.config.concurrency,
                max_keepalive_connections=self.config.concurrency,
            ),
        )
        self._global = asyncio.Semaphore(self.config.concurrency)
        self._hosts: dict[tuple[str, str, int], tuple[asyncio.Semaphore, Breaker]] = {}
        self._active: set[asyncio.Task[object]] = set()
        self._stats = Statistics()
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    def statistics(self) -> Statistics:
        """Return an independent snapshot; completed includes final HTTP error statuses."""
        return replace(self._stats)

    def _origin(self, url: str) -> tuple[str, str, int]:
        parsed = httpx.URL(url)
        if parsed.scheme not in {"http", "https"} or not parsed.host:
            raise ValueError("an absolute HTTP(S) URL is required")
        return parsed.scheme, parsed.host, parsed.port or (443 if parsed.scheme == "https" else 80)

    def circuit_state(self, url: str) -> CircuitState:
        """Observed state; OPEN transitions to HALF_OPEN only on the next admission."""
        entry = self._hosts.get(self._origin(url))
        return entry[1].state if entry else CircuitState.CLOSED

    async def __aenter__(self) -> ResilientClient:
        if self._closed:
            raise RuntimeError("client is closed")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self, *, grace: float = 0.0) -> None:
        """Stop admission, allow grace seconds, then cancel and drain active requests.

        Closing is shielded: cancellation of the caller propagates while cleanup continues.
        The first concurrent close call determines the grace period.
        """
        if not math.isfinite(grace) or grace < 0:
            raise ValueError("grace must be finite and nonnegative")
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._shutdown(grace))
        await asyncio.shield(self._close_task)

    async def _shutdown(self, grace: float) -> None:
        try:
            tasks = set(self._active)
            if tasks:
                _, pending = await asyncio.wait(tasks, timeout=grace)
                for task in pending:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await self._http.aclose()

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        content: bytes | str | None = None,
        retry: bool | None = None,
    ) -> httpx.Response:
        """Return a buffered response, including final 4xx/5xx responses.

        retry=True explicitly permits replay (including POST); retry=False disables it.
        Only replayable bytes/string bodies are accepted. Cancellation always propagates.
        """
        if self._closed:
            raise RuntimeError("client is closed")
        if content is not None and not isinstance(content, (bytes, str)):
            raise TypeError("content must be replayable bytes or str")
        origin = self._origin(url)
        if origin not in self._hosts:
            if len(self._hosts) >= self.config.max_hosts:
                raise HostLimitError("origin registry capacity reached")
            self._hosts[origin] = (
                asyncio.Semaphore(self.config.per_host_concurrency),
                Breaker(self.config.failure_threshold, self.config.recovery_timeout),
            )
        method = method.upper()
        allowed = method in IDEMPOTENT if retry is None else retry
        task = asyncio.create_task(self._request(method, url, headers, content, allowed, origin))
        self._active.add(task)
        self._stats.requests += 1
        try:
            response = await task
            self._stats.completed += 1
            return response
        except asyncio.CancelledError:
            self._stats.cancelled += 1
            raise
        except Exception:
            self._stats.errors += 1
            raise
        finally:
            self._active.discard(task)

    async def _request(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str] | None,
        content: bytes | str | None,
        allowed: bool,
        origin: tuple[str, str, int],
    ) -> httpx.Response:
        semaphore, breaker = self._hosts[origin]
        for attempt in range(self.config.max_retries + 1):
            response: httpx.Response | None = None
            error: Exception | None = None
            # Host first: requests queued on one origin cannot occupy global permits.
            async with semaphore, self._global:
                token = breaker.admit(time.monotonic())
                failed: bool | None = None
                self._stats.attempts += 1
                self._stats.in_flight += 1
                self._stats.peak_in_flight = max(self._stats.peak_in_flight, self._stats.in_flight)
                try:
                    response = await self._send(method, url, headers, content)
                    failed = response.status_code in TRANSIENT
                except RETRY_ERRORS as exc:
                    failed = True
                    error = exc
                finally:
                    self._stats.in_flight -= 1
                    breaker.finish(token, failed, time.monotonic())
            if not failed or not allowed or attempt == self.config.max_retries:
                if error is not None:
                    raise error
                assert response is not None
                return response
            # Full jitter. Retry-After is a floor, never shortened by the backoff cap.
            ceiling = min(
                self.config.backoff_base * (2.0 ** min(attempt, 30)), self.config.backoff_cap
            )
            delay = random.uniform(0, ceiling)
            if response is not None:
                delay = max(delay, retry_after(response.headers.get("Retry-After")) or 0.0)
            self._stats.retries += 1
            self._log.debug("retry scheduled", extra={"attempt": attempt + 1, "delay": delay})
            await asyncio.sleep(delay)
        raise AssertionError("unreachable")

    async def _send(
        self, method: str, url: str, headers: Mapping[str, str] | None, content: bytes | str | None
    ) -> httpx.Response:
        # Wall-clock deadline covers the complete attempt, including body consumption.
        outgoing = httpx.Headers(headers)
        outgoing["Accept-Encoding"] = "identity"
        async with asyncio.timeout(self.config.timeout):
            async with self._http.stream(
                method, url, headers=outgoing, content=content
            ) as response:
                if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                    raise ContentEncodingError("compressed responses are unsupported")
                if response.is_stream_consumed:
                    # MockTransport may supply an already-buffered response.
                    if len(response.content) > self.config.max_response_bytes:
                        raise ResponseTooLargeError("response body limit exceeded")
                    return response
                # Raw bytes avoid unbounded decompression. Decode explicitly outside this library.
                body = bytearray()
                async for chunk in response.aiter_raw():
                    if len(body) + len(chunk) > self.config.max_response_bytes:
                        raise ResponseTooLargeError("response body limit exceeded")
                    body.extend(chunk)
                return httpx.Response(
                    response.status_code,
                    headers=response.headers,
                    content=bytes(body),
                    request=response.request,
                    extensions=response.extensions,
                )

    async def map(
        self, requests: Iterable[Request], *, workers: int | None = None
    ) -> AsyncIterator[Result]:
        """Lazily consume input with at most workers tasks; yield completion-order results.

        Use contextlib.aclosing when stopping iteration early. Per-item exceptions are values.
        """
        count = self.config.concurrency if workers is None else workers
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise ValueError("workers must be a positive integer")
        iterator = iter(requests)
        pending: set[asyncio.Task[Result]] = set()

        async def run(item: Request) -> Result:
            try:
                response = await self.request(
                    item.method,
                    item.url,
                    headers=item.headers,
                    content=item.content,
                    retry=item.retry,
                )
                return Result(item, response=response)
            except Exception as exc:
                return Result(item, error=exc)

        try:
            exhausted = False
            while pending or not exhausted:
                while len(pending) < count and not exhausted:
                    item = next(iterator, None)
                    if item is None:
                        exhausted = True
                    else:
                        pending.add(asyncio.create_task(run(item)))
                if pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    # Keep unconsumed done tasks in the cleanup set across yield suspension.
                    pending.update(done)
                    for task in done:
                        pending.remove(task)
                        yield task.result()
        finally:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
