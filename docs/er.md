# ER-диаграмма

У Flight Service и Booking Service разные экземпляры PostgreSQL. Внешние ключи связывают таблицы внутри одной БД.

## Flight DB

```mermaid
erDiagram
    AIRLINES ||--o{ FLIGHT_NUMBERS : operates
    FLIGHT_NUMBERS ||--o{ FLIGHTS : identifies
    FLIGHTS ||--o{ SEAT_RESERVATIONS : holds
    AIRLINES {
        text code PK
        text name
    }
    FLIGHT_NUMBERS {
        text number PK
        text airline_code FK
    }
    FLIGHTS {
        uuid id PK
        text flight_number FK
        text origin
        text destination
        timestamptz departure_time
        timestamptz arrival_time
        int total_seats
        int available_seats
        bigint price_minor
        text status
    }
    SEAT_RESERVATIONS {
        uuid id PK
        uuid flight_id FK
        uuid booking_id UK
        int seat_count
        text status
    }
```

Уникальный индекс по номеру рейса и дате вылета в UTC запрещает два экземпляра одного рейса за сутки. `CHECK` задаёт формат IATA-кодов, отличные аэропорты вылета и прилёта, прилёт после вылета, положительные вместимость и цену, а также остаток в диапазоне от нуля до вместимости. Статусы ограничены допустимыми значениями.

## Booking DB

```mermaid
erDiagram
    BOOKINGS |o..o| PENDING_BOOKING_OPERATIONS : "logical booking_id"
    BOOKINGS {
        uuid id PK
        text user_id
        uuid flight_id
        text passenger_name
        text passenger_email
        int seat_count
        numeric total_price_minor
        text status
        timestamptz created_at
    }
    PENDING_BOOKING_OPERATIONS {
        uuid booking_id PK
        text action
        timestamptz retry_at
        smallint retry_count
    }
```

Связь с журналом логическая. Запись для возврата мест может существовать до появления бронирования. `bookings.flight_id` и `seat_reservations.booking_id` тоже связываются через API. Ограничение `UNIQUE (booking_id)` на стороне Flight Service гарантирует максимум один резерв на бронирование. Booking Service создаёт подтверждённое бронирование только после успешного резерва.

Схема в 3NF. Название авиакомпании зависит от её кода, поэтому хранится в `airlines`. Принадлежность номера авиакомпании хранится в `flight_numbers`. Остаток мест относится к конкретному вылету и меняется транзакционно. Имя и email описывают пассажира именно этого бронирования. Цена рейса может измениться, а `total_price_minor` фиксирует стоимость при покупке и остаётся прежней.
