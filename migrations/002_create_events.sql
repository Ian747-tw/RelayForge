CREATE TABLE events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload JSONB NOT NULL,
    idempotency_key TEXT NOT NULL,

    CONSTRAINT events_event_type_not_empty
        CHECK (length(trim(event_type)) > 0),

    CONSTRAINT events_idempotency_key_not_empty
        CHECK (length(trim(idempotency_key)) > 0),

    CONSTRAINT events_uq_idempotency_key
        UNIQUE (idempotency_key)

);
