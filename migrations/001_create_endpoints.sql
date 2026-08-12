CREATE TABLE endpoints (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url TEXT NOT NULL,
    secret TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    disabled_at TIMESTAMPTZ,

    CONSTRAINT endpoints_url_not_empty
        CHECK (length(trim(url)) > 0),

    CONSTRAINT endpoints_secret_not_empty
        CHECK (length(secret) > 0)

);
