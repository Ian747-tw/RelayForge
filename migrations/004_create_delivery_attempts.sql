CREATE TABLE delivery_attempts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id BIGINT NOT NULL,
    attempt_number INTEGER NOT NULL,
    response_status INTEGER,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    response_duration_ms INTEGER,

    CONSTRAINT delivery_attempts_fk_Delivery
        FOREIGN KEY (delivery_id)
        REFERENCES deliveries(id)
        ON DELETE RESTRICT,

    CONSTRAINT delivery_attempts_uq_id_attempts
        UNIQUE (delivery_id, attempt_number),

    CONSTRAINT delivery_attempts_attempt_number_valid
        CHECK (attempt_number > 0),

    CONSTRAINT delivery_attempts_response_status_valid
        CHECK (
            (response_status IS NULL)
            OR
            (response_status BETWEEN 100 AND 599)
        ),

    CONSTRAINT delivery_attempts_completed_at_valid
        CHECK (
            (completed_at IS NULL)
            OR
            (completed_at >= started_at)
        ),

    CONSTRAINT delivery_attempts_response_duration_valid
        CHECK (
            (response_duration_ms IS NULL)
            OR
            (response_duration_ms >= 0)
        )


);
