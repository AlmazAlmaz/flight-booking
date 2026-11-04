INSERT INTO airlines (code, name) VALUES ('SU', 'Aeroflot');

INSERT INTO flight_numbers (number, airline_code)
VALUES ('SU1234', 'SU'), ('SU1236', 'SU'), ('SU1235', 'SU'), ('SU1238', 'SU');

INSERT INTO flights (id, flight_number, origin, destination, departure_time, arrival_time,
                     total_seats, available_seats, price_minor, status)
VALUES
    ('00000000-0000-4000-8000-000000000001', 'SU1234', 'SVO', 'LED',
     '2026-04-01T08:00:00Z', '2026-04-01T09:25:00Z', 120, 120, 750000, 'SCHEDULED'),
    ('00000000-0000-4000-8000-000000000002', 'SU1236', 'SVO', 'LED',
     '2026-04-01T18:00:00Z', '2026-04-01T19:25:00Z', 1, 1, 920000, 'SCHEDULED'),
    ('00000000-0000-4000-8000-000000000003', 'SU1235', 'LED', 'SVO',
     '2026-04-02T10:00:00Z', '2026-04-02T11:25:00Z', 80, 80, 810000, 'SCHEDULED'),
    ('00000000-0000-4000-8000-000000000004', 'SU1238', 'SVO', 'LED',
     '2026-04-01T12:00:00Z', '2026-04-01T13:25:00Z', 100, 100, 620000, 'CANCELLED');
