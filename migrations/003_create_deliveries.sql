CREATE TABLE deliveries (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id BIGINT NOT NULL,
    endpoint_id BIGINT NOT NULL,
    next_attempt_due TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL DEFAULT 'pending',
    attempts_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,

    CONSTRAINT deliveries_fk_event_id
        FOREIGN KEY (event_id)
        REFERENCES events(id)
        ON DELETE RESTRICT,

    CONSTRAINT deliveries_fk_endpoint_id
        FOREIGN KEY (endpoint_id)
        REFERENCES endpoints(id)
        ON DELETE RESTRICT,

    CONSTRAINT deliveries_uq_event_endpoint
        UNIQUE (event_id, endpoint_id),

    CONSTRAINT deliveries_status_is_valid
        CHECK (
            status IN (
                'pending',
                'retry_scheduled',
                'processing',
                'success',
                'dead'
            )
        ),

    CONSTRAINT deliveries_attempts_count_valid
        CHECK (attempts_count >= 0),

    CONSTRAINT deliveries_completed_at_valid
        CHECK (
            (status IN ('success', 'dead') AND completed_at IS NOT NULL)
            OR
            (status NOT IN ('success', 'dead') AND completed_at IS NULL)
        )
);
