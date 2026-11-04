CREATE TABLE pending_booking_operations (
    booking_id UUID PRIMARY KEY,
    action TEXT NOT NULL CHECK (action IN ('RELEASE_ORPHAN', 'CANCEL')),
    retry_at TIMESTAMPTZ NOT NULL,
    retry_count SMALLINT NOT NULL DEFAULT 0 CHECK (retry_count BETWEEN 0 AND 4)
);

CREATE INDEX pending_booking_operations_retry_idx
    ON pending_booking_operations (retry_at, booking_id);
