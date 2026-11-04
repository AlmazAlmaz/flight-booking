import logging
import math
import time
from collections import deque
from enum import StrEnum
from threading import Lock

import grpc

logger = logging.getLogger(__name__)

RETRYABLE_CODES = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}
BREAKER_FAILURE_CODES = RETRYABLE_CODES | {
    grpc.StatusCode.INTERNAL,
    grpc.StatusCode.UNKNOWN,
    grpc.StatusCode.DATA_LOSS,
}
RETRY_DELAYS = (0.1, 0.2)


class CircuitState(StrEnum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitOpenError(grpc.RpcError):
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.UNAVAILABLE

    def details(self) -> str:
        return str(self)


class CircuitBreaker:
    def __init__(self, failure_threshold: int, open_seconds: float, window_seconds: float):
        if failure_threshold < 1:
            raise ValueError("CIRCUIT_FAILURE_THRESHOLD must be at least 1")
        if not math.isfinite(open_seconds) or open_seconds <= 0:
            raise ValueError("CIRCUIT_OPEN_SECONDS must be positive and finite")
        if not math.isfinite(window_seconds) or window_seconds <= 0:
            raise ValueError("CIRCUIT_WINDOW_SECONDS must be positive and finite")
        self._threshold = failure_threshold
        self._open_seconds = open_seconds
        self._window_seconds = window_seconds
        self._lock = Lock()
        self._state = CircuitState.CLOSED
        self._generation = 0
        self._retry_at = 0.0
        self._failures: deque[float] = deque()

    def _transition(self, state: CircuitState, now: float) -> None:
        logger.warning("Flight circuit breaker: %s -> %s", self._state, state)
        self._state = state
        self._generation += 1
        self._failures.clear()
        if state == CircuitState.OPEN:
            self._retry_at = now + self._open_seconds

    def admit(self) -> tuple[int, bool]:
        """Return the state generation and whether this call is the probe."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                raise CircuitOpenError("Flight Service recovery probe is in progress")
            if self._state == CircuitState.OPEN:
                now = time.monotonic()
                if now < self._retry_at:
                    raise CircuitOpenError(
                        "Flight Service is temporarily unavailable (circuit breaker open)"
                    )
                self._transition(CircuitState.HALF_OPEN, now)
            return self._generation, self._state == CircuitState.HALF_OPEN

    def record(self, generation: int, code: grpc.StatusCode) -> None:
        with self._lock:
            if generation != self._generation:
                # A call admitted before a state change cannot affect the new state.
                return
            now = time.monotonic()
            failed = code in BREAKER_FAILURE_CODES
            if self._state == CircuitState.HALF_OPEN:
                self._transition(CircuitState.OPEN if failed else CircuitState.CLOSED, now)
            elif failed:
                cutoff = now - self._window_seconds
                while self._failures and self._failures[0] < cutoff:
                    self._failures.popleft()
                self._failures.append(now)
                if len(self._failures) >= self._threshold:
                    self._transition(CircuitState.OPEN, now)


class FlightResilienceInterceptor(grpc.UnaryUnaryClientInterceptor):
    def __init__(self, breaker: CircuitBreaker):
        self._breaker = breaker

    def intercept_unary_unary(self, continuation, client_call_details, request):
        generation, is_probe = self._breaker.admit()
        delays = () if is_probe else RETRY_DELAYS
        code = grpc.StatusCode.UNKNOWN
        try:
            for attempt in range(len(delays) + 1):
                outcome = continuation(client_call_details, request)
                code = outcome.code()
                if code not in RETRYABLE_CODES or attempt == len(delays):
                    # Return the Call/Future so gRPC propagates its response or RpcError.
                    return outcome
                delay = delays[attempt]
                logger.info(
                    "Retrying %s after %s in %.1fs (attempt %d)",
                    client_call_details.method,
                    code.name,
                    delay,
                    attempt + 2,
                )
                time.sleep(delay)
        finally:
            self._breaker.record(generation, code)
