# Verification record

Verified locally on 2026-10-27, Linux, CPython 3.12.14, HTTPX 0.28.1:

- Editable installation with development extras succeeded.
- `python -m pytest -q`: **33 passed**.
- `python -m ruff check .`: passed.
- `python -m ruff format --check .`: passed.
- `python -m mypy`: strict checking passed for all three package modules.
- `python examples/local_demo.py`: real loopback socket demo completed with 9
  attempts, 5 retry waits, 3 completed responses, 1 expected TimeoutError, and 0
  remaining in-flight requests. HTTP 429 waited for the server's Retry-After.
- `python -m build`: wheel and source distribution built successfully; the wheel
  was built from the source distribution.
- Built wheel installed into a separate virtual environment; import from outside
  the source tree resolved to its installed site-packages and version 0.1.0.

The tests use HTTPX MockTransport streaming responses for deterministic timing
and failures. The demo separately verifies actual HTTP over local sockets.
Breaker tests use a controlled clock instead of flaky long sleeps. Cancellation
is tested during active network operations, semaphore waits, batch consumption,
and Retry-After backoff. Tests also verify one half-open probe, rejection of
compressed bodies, cookie rejection, and response/host limits.

Python 3.13 and 3.14 are configured in GitHub Actions but were **not run locally**.
GitHub Actions itself was not run. No external-service integration, TLS fault
injection, multi-process testing, long-duration load testing, or production
certification was performed. The package has not been published to PyPI or GitHub.
