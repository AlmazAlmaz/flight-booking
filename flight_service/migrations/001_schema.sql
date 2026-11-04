CREATE TABLE airlines (
    code text PRIMARY KEY CHECK (code ~ '^[A-Z0-9]{2}$'),
    name text NOT NULL CHECK (btrim(name) <> '')
);

CREATE TABLE flight_numbers (
    number text PRIMARY KEY CHECK (btrim(number) <> ''),
    airline_code text NOT NULL REFERENCES airlines (code)
);

CREATE TABLE flights (
    id uuid PRIMARY KEY,
    flight_number text NOT NULL REFERENCES flight_numbers (number),
    origin text NOT NULL CHECK (origin ~ '^[A-Z]{3}$'),
    destination text NOT NULL CHECK (destination ~ '^[A-Z]{3}$'),
    departure_time timestamptz NOT NULL,
    arrival_time timestamptz NOT NULL,
    total_seats integer NOT NULL CHECK (total_seats > 0),
    available_seats integer NOT NULL CHECK (available_seats BETWEEN 0 AND total_seats),
    price_minor bigint NOT NULL CHECK (price_minor > 0),
    status text NOT NULL CHECK (status IN ('SCHEDULED', 'DEPARTED', 'CANCELLED', 'COMPLETED')),
    CHECK (origin <> destination),
    CHECK (arrival_time > departure_time)
);

CREATE UNIQUE INDEX flights_number_departure_date_unique
    ON flights (flight_number, ((departure_time AT TIME ZONE 'UTC')::date));

CREATE INDEX flights_scheduled_route
    ON flights (origin, destination, departure_time)
    WHERE status = 'SCHEDULED';

CREATE TABLE seat_reservations (
    id uuid PRIMARY KEY,
    flight_id uuid NOT NULL REFERENCES flights (id),
    booking_id uuid NOT NULL UNIQUE,
    seat_count integer NOT NULL CHECK (seat_count > 0),
    status text NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED', 'EXPIRED'))
);

CREATE INDEX seat_reservations_flight ON seat_reservations (flight_id);
