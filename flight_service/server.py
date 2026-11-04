import logging
import os
import re
import secrets
import signal
from concurrent.futures import ThreadPoolExecutor
from datetime import timezone
from pathlib import Path
from uuid import UUID, uuid4

import grpc
import psycopg
from google.protobuf.timestamp_pb2 import Timestamp
from psycopg.rows import dict_row

import flight_pb2 as pb
import flight_pb2_grpc as rpc
from common.migrations import migrate
from flight_service.cache import FlightCache, flight_key, search_key


log = logging.getLogger(__name__)
code = grpc.StatusCode

SELECT_FLIGHT = """
    SELECT f.*, a.name AS airline
    FROM flights AS f
    JOIN flight_numbers AS n ON n.number = f.flight_number
    JOIN airlines AS a ON a.code = n.airline_code
"""


def parse_uuid(value, field, context):
    try:
        return UUID(value)
    except ValueError:
        context.abort(code.INVALID_ARGUMENT, f"{field} must be a UUID")


def departure_date(request, context):
    if not request.HasField("departure_date"):
        return None
    timestamp = request.departure_date
    if timestamp.seconds % 86_400 or timestamp.nanos:
        context.abort(code.INVALID_ARGUMENT, "departure_date must be midnight UTC")
    try:
        return timestamp.ToDatetime(tzinfo=timezone.utc)
    except (ValueError, OverflowError):
        context.abort(code.INVALID_ARGUMENT, "departure_date is outside the Timestamp range")


def to_timestamp(value):
    result = Timestamp()
    result.FromDatetime(value)
    return result


def flight_message(row):
    return pb.Flight(
        id=str(row["id"]),
        flight_number=row["flight_number"],
        airline=row["airline"],
        origin=row["origin"],
        destination=row["destination"],
        departure_time=to_timestamp(row["departure_time"]),
        arrival_time=to_timestamp(row["arrival_time"]),
        total_seats=row["total_seats"],
        available_seats=row["available_seats"],
        price_minor=row["price_minor"],
        status=pb.FlightStatus.Value(f"FLIGHT_STATUS_{row['status']}"),
    )


def reservation_message(row):
    return pb.SeatReservation(
        id=str(row["id"]),
        flight_id=str(row["flight_id"]),
        booking_id=str(row["booking_id"]),
        seat_count=row["seat_count"],
        status=pb.ReservationStatus.Value(f"RESERVATION_STATUS_{row['status']}"),
    )


class FlightMiddleware(grpc.ServerInterceptor):
    def __init__(self, api_key):
        if not api_key:
            raise ValueError("API_KEY must be non-empty")
        self._api_key = api_key.encode()

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None
        keys = [
            value for key, value in handler_call_details.invocation_metadata
            if key == "x-api-key"
        ]

        def call(request, context):
            if len(keys) != 1 or not secrets.compare_digest(keys[0].encode(), self._api_key):
                context.abort(code.UNAUTHENTICATED, "Missing or invalid API key")
            try:
                return handler.unary_unary(request, context)
            except (psycopg.OperationalError, psycopg.InterfaceError):
                log.exception("PostgreSQL unavailable during %s", handler_call_details.method)
                context.abort(code.UNAVAILABLE, "Flight database unavailable")
            except psycopg.Error:
                log.exception("PostgreSQL error during %s", handler_call_details.method)
                context.abort(code.INTERNAL, "Flight database error")

        return grpc.unary_unary_rpc_method_handler(
            call,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


def lock_booking(conn, booking_id):
    # Both mutations lock the same booking before touching its reservation.
    conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (str(booking_id),))


def lock_flight(conn, flight_id):
    return conn.execute(
        """
        SELECT id, status, available_seats, origin, destination,
               (departure_time AT TIME ZONE 'UTC')::date AS departure_day
        FROM flights WHERE id = %s FOR UPDATE
        """,
        (flight_id,),
    ).fetchone()


def check_active(context):
    # A deadline may expire while waiting for a row or advisory lock.
    remaining = context.time_remaining()
    if remaining is not None and remaining <= 0:
        context.abort(code.DEADLINE_EXCEEDED, "RPC deadline expired before commit")
    if not context.is_active():
        context.abort(code.CANCELLED, "RPC cancelled before commit")


class FlightService(rpc.FlightServiceServicer):
    def __init__(self, database_url, cache):
        self._database_url = database_url
        self._cache = cache

    def _connect(self):
        return psycopg.connect(
            self._database_url,
            row_factory=dict_row,
            connect_timeout=3,
            options="-c lock_timeout=4000 -c statement_timeout=8000",
        )

    def SearchFlights(self, request, context):
        origin, destination = request.origin, request.destination
        if (not re.fullmatch(r"[A-Z]{3}", origin) or not re.fullmatch(r"[A-Z]{3}", destination)
                or origin == destination):
            context.abort(code.INVALID_ARGUMENT, "Specify two distinct three-letter IATA codes")
        day = departure_date(request, context)
        key = search_key(origin, destination, day.date() if day is not None else None)

        def load():
            query = SELECT_FLIGHT + "WHERE f.origin = %s AND f.destination = %s AND f.status = 'SCHEDULED'"
            params = [origin, destination]
            if day is not None:
                query += " AND f.departure_time >= %s AND f.departure_time < %s + interval '24 hours'"
                params.extend((day, day))
            query += " ORDER BY f.departure_time, f.id"
            with self._connect() as conn:
                rows = conn.execute(query, params).fetchall()
            return pb.SearchFlightsResponse(flights=[flight_message(row) for row in rows])

        return self._cache.read(key, pb.SearchFlightsResponse, load)

    def GetFlight(self, request, context):
        flight_id = parse_uuid(request.flight_id, "flight_id", context)

        def load():
            with self._connect() as conn:
                row = conn.execute(SELECT_FLIGHT + "WHERE f.id = %s", (flight_id,)).fetchone()
            if row is None:
                context.abort(code.NOT_FOUND, "Flight not found")
            return pb.GetFlightResponse(flight=flight_message(row))

        return self._cache.read(flight_key(flight_id), pb.GetFlightResponse, load)

    def UpdateFlight(self, request, context):
        flight_id = parse_uuid(request.flight_id, "flight_id", context)
        price = request.price_minor if request.HasField("price_minor") else None
        status = None
        if request.HasField("status"):
            try:
                status = pb.FlightStatus.Name(request.status).removeprefix("FLIGHT_STATUS_")
            except ValueError:
                context.abort(code.INVALID_ARGUMENT, "Unknown flight status")
            if status == "UNSPECIFIED":
                context.abort(code.INVALID_ARGUMENT, "A concrete flight status is required")
        if price is None and status is None:
            context.abort(code.INVALID_ARGUMENT, "Specify price_minor or status")
        if price is not None and price <= 0:
            context.abort(code.INVALID_ARGUMENT, "price_minor must be positive")

        changed_flight = None
        try:
            with self._connect() as conn:
                flight = lock_flight(conn, flight_id)
                if flight is None:
                    context.abort(code.NOT_FOUND, "Flight not found")
                check_active(context)
                changed_flight = flight
                conn.execute(
                    """
                    UPDATE flights
                    SET price_minor = COALESCE(%s, price_minor), status = COALESCE(%s, status)
                    WHERE id = %s
                    """,
                    (price, status, flight_id),
                )
                row = conn.execute(SELECT_FLIGHT + "WHERE f.id = %s", (flight_id,)).fetchone()
        finally:
            if changed_flight is not None:
                self._cache.invalidate_flight(changed_flight)
        return pb.UpdateFlightResponse(flight=flight_message(row))

    def ReserveSeats(self, request, context):
        flight_id = parse_uuid(request.flight_id, "flight_id", context)
        booking_id = parse_uuid(request.booking_id, "booking_id", context)
        if request.seat_count <= 0:
            context.abort(code.INVALID_ARGUMENT, "seat_count must be positive")

        changed_flight = None
        try:
            # The connection commits on success and rolls back on any exception.
            with self._connect() as conn:
                lock_booking(conn, booking_id)
                existing = conn.execute(
                    "SELECT * FROM seat_reservations WHERE booking_id = %s", (booking_id,),
                ).fetchone()
                if existing is not None:
                    if existing["flight_id"] != flight_id or existing["seat_count"] != request.seat_count:
                        context.abort(code.ALREADY_EXISTS, "booking_id already has a different reservation")
                    if existing["status"] != "ACTIVE":
                        context.abort(code.FAILED_PRECONDITION, "Reservation is no longer active")
                    return pb.ReserveSeatsResponse(reservation=reservation_message(existing))

                flight = lock_flight(conn, flight_id)
                if flight is None:
                    context.abort(code.NOT_FOUND, "Flight not found")
                if flight["status"] != "SCHEDULED":
                    context.abort(code.FAILED_PRECONDITION, "Flight is not scheduled")
                if flight["available_seats"] < request.seat_count:
                    context.abort(code.RESOURCE_EXHAUSTED, "Not enough available seats")

                check_active(context)
                changed_flight = flight
                conn.execute(
                    "UPDATE flights SET available_seats = available_seats - %s WHERE id = %s",
                    (request.seat_count, flight_id),
                )
                reservation = conn.execute(
                    """
                    INSERT INTO seat_reservations (id, flight_id, booking_id, seat_count, status)
                    VALUES (%s, %s, %s, %s, 'ACTIVE') RETURNING *
                    """,
                    (uuid4(), flight_id, booking_id, request.seat_count),
                ).fetchone()
                check_active(context)
        finally:
            # Invalidate after leaving the transaction, even if COMMIT's outcome is unknown.
            if changed_flight is not None:
                self._cache.invalidate_flight(changed_flight)
        return pb.ReserveSeatsResponse(reservation=reservation_message(reservation))

    def ReleaseReservation(self, request, context):
        booking_id = parse_uuid(request.booking_id, "booking_id", context)
        changed_flight = None
        try:
            with self._connect() as conn:
                lock_booking(conn, booking_id)
                reservation = conn.execute(
                    "SELECT * FROM seat_reservations WHERE booking_id = %s FOR UPDATE", (booking_id,),
                ).fetchone()
                if reservation is None:
                    context.abort(code.NOT_FOUND, "Reservation not found")
                if reservation["status"] == "RELEASED":
                    return pb.ReleaseReservationResponse(reservation=reservation_message(reservation))
                if reservation["status"] != "ACTIVE":
                    context.abort(code.FAILED_PRECONDITION, "Reservation is no longer active")

                flight = lock_flight(conn, reservation["flight_id"])
                check_active(context)
                changed_flight = flight
                conn.execute(
                    "UPDATE flights SET available_seats = available_seats + %s WHERE id = %s",
                    (reservation["seat_count"], reservation["flight_id"]),
                )
                reservation = conn.execute(
                    "UPDATE seat_reservations SET status = 'RELEASED' WHERE booking_id = %s RETURNING *",
                    (booking_id,),
                ).fetchone()
        finally:
            if changed_flight is not None:
                self._cache.invalidate_flight(changed_flight)
        return pb.ReleaseReservationResponse(reservation=reservation_message(reservation))


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    database_url = os.environ["DATABASE_URL"]
    auth = FlightMiddleware(os.environ["API_KEY"])
    cache = FlightCache.from_env()
    migrate(database_url, Path(__file__).with_name("migrations"))

    server = grpc.server(ThreadPoolExecutor(max_workers=16), interceptors=(auth,))
    rpc.add_FlightServiceServicer_to_server(FlightService(database_url, cache), server)
    address = f"[::]:{os.getenv('GRPC_PORT', '50051')}"
    server.add_insecure_port(address)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: server.stop(grace=5))
    server.start()
    log.info("Flight Service listening on %s", address)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
