"""Public REST API and the booking workflow."""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, time
from pathlib import Path
from threading import Event, Thread
from typing import Annotated, Literal
from uuid import UUID, uuid4

import grpc
import psycopg
from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr, Field, StringConstraints

import flight_pb2 as pb
from booking_service.flight_client import FlightClient
from booking_service.operations import complete_cancellation, delete_operation, open_database, run_recovery
from common.migrations import migrate

logger = logging.getLogger(__name__)

UserId = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]
PassengerName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=120)]
AirportCode = Annotated[str, StringConstraints(pattern=r"^[A-Z]{3}$")]
SeatCount = Annotated[int, Field(strict=True, gt=0, le=2**31 - 1)]

GRPC_TO_HTTP = {
    grpc.StatusCode.INVALID_ARGUMENT: 400,
    grpc.StatusCode.NOT_FOUND: 404,
    grpc.StatusCode.ALREADY_EXISTS: 409,
    grpc.StatusCode.FAILED_PRECONDITION: 409,
    grpc.StatusCode.RESOURCE_EXHAUSTED: 409,
    grpc.StatusCode.UNAVAILABLE: 503,
    grpc.StatusCode.DEADLINE_EXCEEDED: 504,
}


class BookingCreate(BaseModel):
    user_id: UserId
    flight_id: UUID
    passenger_name: PassengerName
    passenger_email: EmailStr
    seat_count: SeatCount


class BookingResponse(BaseModel):
    id: UUID
    user_id: str
    flight_id: UUID
    passenger_name: str
    passenger_email: str
    seat_count: int
    total_price: str
    status: Literal["CONFIRMED", "CANCELLED"]
    created_at: datetime


class FlightResponse(BaseModel):
    id: UUID
    flight_number: str
    airline: str
    origin: str
    destination: str
    departure_time: datetime
    arrival_time: datetime
    total_seats: int
    available_seats: int
    price: str
    status: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO)
    database_url = os.environ["DATABASE_URL"]
    migrate(database_url, Path(__file__).with_name("migrations"))
    app.state.database_url = database_url
    client = FlightClient()
    app.state.flight_client = client
    stop = Event()
    recovery = Thread(target=run_recovery, args=(database_url, client, stop), name="booking-recovery", daemon=True)
    recovery.start()
    try:
        yield
    finally:
        stop.set()
        client.close()
        await asyncio.to_thread(recovery.join)


app = FastAPI(title="Booking Service", lifespan=lifespan)


@app.exception_handler(grpc.RpcError)
async def flight_error(_request: Request, exc: grpc.RpcError) -> JSONResponse:
    code = exc.code()
    http_status = GRPC_TO_HTTP.get(code, status.HTTP_502_BAD_GATEWAY)
    if http_status == status.HTTP_502_BAD_GATEWAY:
        logger.error("Flight Service RPC failed: %s", code.name)
        detail = "Flight Service could not process the request"
    else:
        detail = exc.details() or code.name
    return JSONResponse(
        status_code=http_status,
        content={"detail": detail, "grpc_code": code.name},
    )


@app.exception_handler(psycopg.Error)
async def database_error(_request: Request, exc: psycopg.Error) -> JSONResponse:
    logger.error("Booking database operation failed", exc_info=exc)
    if isinstance(exc, psycopg.OperationalError):
        return JSONResponse(status_code=503, content={"detail": "Booking database is unavailable"})
    return JSONResponse(status_code=500, content={"detail": "Booking database operation failed"})


def connect(request: Request):
    return open_database(request.app.state.database_url)


def rubles(kopecks: int) -> str:
    whole, fraction = divmod(kopecks, 100)
    return f"{whole}.{fraction:02d}"


def flight_response(flight: pb.Flight) -> FlightResponse:
    return FlightResponse(
        id=flight.id,
        flight_number=flight.flight_number,
        airline=flight.airline,
        origin=flight.origin,
        destination=flight.destination,
        departure_time=flight.departure_time.ToDatetime(tzinfo=UTC),
        arrival_time=flight.arrival_time.ToDatetime(tzinfo=UTC),
        total_seats=flight.total_seats,
        available_seats=flight.available_seats,
        price=rubles(flight.price_minor),
        status=pb.FlightStatus.Name(flight.status).removeprefix("FLIGHT_STATUS_"),
    )


def booking_response(row: dict) -> BookingResponse:
    data = row.copy()
    data["total_price"] = rubles(int(data.pop("total_price_minor")))
    return BookingResponse.model_validate(data)


@app.get("/health")
def health(request: Request):
    with connect(request) as connection:
        connection.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/flights")
def search_flights(
    request: Request,
    origin: Annotated[AirportCode, Query()],
    destination: Annotated[AirportCode, Query()],
    flight_date: Annotated[date | None, Query(alias="date")] = None,
) -> list[FlightResponse]:
    query = pb.SearchFlightsRequest(origin=origin, destination=destination)
    if flight_date is not None:
        query.departure_date.FromDatetime(datetime.combine(flight_date, time.min, tzinfo=UTC))
    result = request.app.state.flight_client.search_flights(query)
    return [flight_response(flight) for flight in result.flights]


@app.get("/flights/{id}")
def get_flight(id: UUID, request: Request) -> FlightResponse:
    result = request.app.state.flight_client.get_flight(pb.GetFlightRequest(flight_id=str(id)))
    return flight_response(result.flight)


@app.post("/bookings", status_code=status.HTTP_201_CREATED)
def create_booking(body: BookingCreate, request: Request) -> BookingResponse:
    client = request.app.state.flight_client
    flight_id = str(body.flight_id)
    flight = client.get_flight(pb.GetFlightRequest(flight_id=flight_id)).flight
    total_price_minor = body.seat_count * flight.price_minor

    # Generate the ID once. The gRPC client retries with the same request.
    booking_id = uuid4()
    with connect(request) as connection:
        # Commit the recovery intent before a remote reservation can exist.
        connection.execute(
            """
            INSERT INTO pending_booking_operations (booking_id, action, retry_at)
            VALUES (%s, 'RELEASE_ORPHAN', clock_timestamp() + INTERVAL '5 seconds')
            """,
            (booking_id,),
        )

    with connect(request) as connection:
        operation = connection.execute(
            """
            SELECT booking_id FROM pending_booking_operations
            WHERE booking_id = %s AND action = 'RELEASE_ORPHAN'
            FOR UPDATE
            """,
            (booking_id,),
        ).fetchone()
        if operation is None:
            # Recovery took over before this transaction could start.
            raise HTTPException(status_code=503, detail="Booking creation expired. Try again")
        client.reserve_seats(
            pb.ReserveSeatsRequest(flight_id=flight_id, booking_id=str(booking_id), seat_count=body.seat_count)
        )
        inserted = connection.execute(
            """
            INSERT INTO bookings (
                id, user_id, flight_id, passenger_name, passenger_email,
                seat_count, total_price_minor, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'CONFIRMED')
            RETURNING *
            """,
            (
                booking_id, body.user_id, body.flight_id, body.passenger_name,
                body.passenger_email, body.seat_count, total_price_minor,
            ),
        ).fetchone()
        # Confirmation and disarming recovery commit together.
        delete_operation(connection, booking_id)
    return booking_response(inserted)


@app.get("/bookings")
def list_bookings(request: Request, user_id: Annotated[UserId, Query()]) -> list[BookingResponse]:
    with connect(request) as connection:
        rows = connection.execute(
            "SELECT * FROM bookings WHERE user_id = %s ORDER BY created_at DESC, id",
            (user_id,),
        ).fetchall()
    return [booking_response(row) for row in rows]


@app.get("/bookings/{id}")
def get_booking(id: UUID, request: Request) -> BookingResponse:
    with connect(request) as connection:
        row = connection.execute("SELECT * FROM bookings WHERE id = %s", (id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Booking not found")
    return booking_response(row)


@app.post("/bookings/{id}/cancel")
def cancel_booking(id: UUID, request: Request) -> BookingResponse:
    # INSERT ... SELECT reads the booking without locking it. The execution
    # transaction always locks the pending operation before the booking.
    with connect(request) as connection:
        connection.execute(
            """
            INSERT INTO pending_booking_operations (booking_id, action, retry_at)
            SELECT id, 'CANCEL', clock_timestamp() + INTERVAL '1 second'
            FROM bookings WHERE id = %s AND status = 'CONFIRMED'
            ON CONFLICT (booking_id) DO NOTHING
            """,
            (id,),
        )
    with connect(request) as connection:
        operation = connection.execute(
            "SELECT booking_id FROM pending_booking_operations WHERE booking_id = %s AND action = 'CANCEL' FOR UPDATE",
            (id,),
        ).fetchone()
        if operation is None:
            row = connection.execute(
                "SELECT * FROM bookings WHERE id = %s",
                (id,),
            ).fetchone()
        else:
            row = complete_cancellation(connection, request.app.state.flight_client, id)
            delete_operation(connection, id)
    if row is None:
        raise HTTPException(status_code=404, detail="Booking not found")
    if row["status"] != "CANCELLED":
        raise HTTPException(status_code=503, detail="Cancellation could not be scheduled. Try again")
    return booking_response(row)
