"""Durable recovery for operations that involve both service databases."""

import logging
from threading import Event
from uuid import UUID

import grpc
import psycopg
from psycopg.rows import dict_row

import flight_pb2 as pb
from booking_service.flight_client import FlightClient

logger = logging.getLogger(__name__)


def open_database(database_url: str):
    return psycopg.connect(database_url, connect_timeout=5, row_factory=dict_row)


def delete_operation(connection: psycopg.Connection, booking_id: UUID) -> None:
    connection.execute("DELETE FROM pending_booking_operations WHERE booking_id = %s", (booking_id,))


def complete_cancellation(connection: psycopg.Connection, client: FlightClient, booking_id: UUID):
    # The caller already holds the pending-operation lock. Both the API and
    # recovery worker acquire locks in that order.
    row = connection.execute("SELECT * FROM bookings WHERE id = %s FOR UPDATE", (booking_id,)).fetchone()
    if row is None or row["status"] == "CANCELLED":
        return row
    client.release_reservation(pb.ReleaseReservationRequest(booking_id=str(booking_id)))
    return connection.execute(
        "UPDATE bookings SET status = 'CANCELLED' WHERE id = %s RETURNING *",
        (booking_id,),
    ).fetchone()


def recover_one(database_url: str, client: FlightClient) -> bool:
    with open_database(database_url) as connection:
        operation = connection.execute(
            """
            SELECT booking_id, action, retry_count
            FROM pending_booking_operations
            WHERE retry_at <= clock_timestamp()
            ORDER BY retry_at, booking_id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """
        ).fetchone()
        if operation is None:
            return False
        booking_id = operation["booking_id"]
        action = operation["action"]
        try:
            if action == "CANCEL":
                complete_cancellation(connection, client, booking_id)
            else:
                # A failed create may never have made it to Flight Service.
                try:
                    client.release_reservation(pb.ReleaseReservationRequest(booking_id=str(booking_id)))
                except grpc.RpcError as exc:
                    if exc.code() != grpc.StatusCode.NOT_FOUND:
                        raise
        except grpc.RpcError as exc:
            retry_count = min(operation["retry_count"] + 1, 4)
            delay = 2**retry_count
            connection.execute(
                """
                UPDATE pending_booking_operations
                SET retry_count = %s, retry_at = clock_timestamp() + make_interval(secs => %s)
                WHERE booking_id = %s
                """,
                (retry_count, delay, booking_id),
            )
            logger.warning("Recovery %s for %s returned %s. Retry in %ss", action, booking_id, exc.code().name, delay)
        else:
            delete_operation(connection, booking_id)
            logger.info("Recovered booking operation %s for %s", action, booking_id)
    return True


def run_recovery(database_url: str, client: FlightClient, stop: Event) -> None:
    while not stop.is_set():
        try:
            if recover_one(database_url, client):
                continue
        except psycopg.Error:
            logger.exception("Booking recovery could not access PostgreSQL")
        except ValueError:
            # The channel can close during shutdown while a recovery RPC starts.
            if not stop.is_set():
                logger.exception("Booking recovery could not call Flight Service")
        stop.wait(1.0)
