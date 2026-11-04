CREATE TABLE bookings (
    id UUID PRIMARY KEY,
    user_id TEXT NOT NULL CHECK (char_length(btrim(user_id)) BETWEEN 1 AND 128),
    flight_id UUID NOT NULL,
    passenger_name TEXT NOT NULL CHECK (char_length(btrim(passenger_name)) BETWEEN 1 AND 120),
    passenger_email TEXT NOT NULL CHECK (char_length(btrim(passenger_email)) BETWEEN 1 AND 320),
    seat_count INTEGER NOT NULL CHECK (seat_count > 0),
    total_price_minor NUMERIC(29, 0) NOT NULL CHECK (total_price_minor > 0),
    status TEXT NOT NULL CHECK (status IN ('CONFIRMED', 'CANCELLED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX bookings_user_id_created_at_idx ON bookings (user_id, created_at DESC, id);
