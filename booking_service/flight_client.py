import math
import os

import grpc

import flight_pb2 as pb
from flight_pb2_grpc import FlightServiceStub

from booking_service.resilience import CircuitBreaker, FlightResilienceInterceptor


class FlightClient:
    def __init__(self):
        api_key = os.environ["API_KEY"]
        if not api_key:
            raise ValueError("API_KEY must not be empty")
        self._metadata = (("x-api-key", api_key),)
        self._timeout = float(os.getenv("FLIGHT_RPC_TIMEOUT_SECONDS", "1.0"))
        if not math.isfinite(self._timeout) or self._timeout <= 0:
            raise ValueError("FLIGHT_RPC_TIMEOUT_SECONDS must be positive and finite")

        breaker = CircuitBreaker(
            failure_threshold=int(os.getenv("CIRCUIT_FAILURE_THRESHOLD", "5")),
            open_seconds=float(os.getenv("CIRCUIT_OPEN_SECONDS", "15")),
            window_seconds=float(os.getenv("CIRCUIT_WINDOW_SECONDS", "30")),
        )
        target = os.getenv("FLIGHT_TARGET", "flight-service:50051")
        # The interceptor owns the retry policy and its attempt limit.
        channel = grpc.insecure_channel(target, options=(("grpc.enable_retries", 0),))
        self._channel = grpc.intercept_channel(channel, FlightResilienceInterceptor(breaker))
        self._stub = FlightServiceStub(self._channel)

    def _call(self, rpc, request):
        return rpc(request, timeout=self._timeout, metadata=self._metadata)

    def search_flights(self, request: pb.SearchFlightsRequest) -> pb.SearchFlightsResponse:
        return self._call(self._stub.SearchFlights, request)

    def get_flight(self, request: pb.GetFlightRequest) -> pb.GetFlightResponse:
        return self._call(self._stub.GetFlight, request)

    def reserve_seats(self, request: pb.ReserveSeatsRequest) -> pb.ReserveSeatsResponse:
        return self._call(self._stub.ReserveSeats, request)

    def release_reservation(
        self, request: pb.ReleaseReservationRequest
    ) -> pb.ReleaseReservationResponse:
        return self._call(self._stub.ReleaseReservation, request)

    def close(self) -> None:
        self._channel.close()
