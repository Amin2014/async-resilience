"""Event-loop confined breaker; admission and completion never suspend."""

from enum import StrEnum


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(RuntimeError):
    """An origin's circuit is open or its recovery probe is already running."""


class Breaker:
    def __init__(self, threshold: int, recovery: float) -> None:
        self.threshold = threshold
        self.recovery = recovery
        self.state = CircuitState.CLOSED
        self.failures = 0
        self.opened_at = 0.0
        self.epoch = 0
        self.probing = False

    def admit(self, now: float) -> int:
        if self.state == CircuitState.OPEN:
            if now - self.opened_at < self.recovery:
                raise CircuitOpenError("circuit is open")
            self.state = CircuitState.HALF_OPEN
        if self.state == CircuitState.HALF_OPEN:
            if self.probing:
                raise CircuitOpenError("recovery probe is already running")
            self.probing = True
        return self.epoch

    def finish(self, epoch: int, failed: bool | None, now: float) -> None:
        if epoch != self.epoch:
            return  # A late completion must not close a newer circuit.
        if failed is None:
            self.probing = False  # Cancellation neither heals nor penalizes the origin.
            return
        if self.state == CircuitState.HALF_OPEN:
            self.probing = False
            if not failed:
                self.state = CircuitState.CLOSED
                self.failures = 0
                self.epoch += 1
                return
        elif not failed:
            self.failures = 0
            return
        self.failures += 1
        if self.state == CircuitState.HALF_OPEN or self.failures >= self.threshold:
            self.state = CircuitState.OPEN
            self.opened_at = now
            self.epoch += 1
