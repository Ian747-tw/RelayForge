\set ON_ERROR_STOP on

BEGIN;

INSERT INTO endpoints (url, secret)
VALUES
    ('example-1.com', 'secret-1'),
    ('example-2.com', 'secret-2')
RETURNING id, url, created_at;

INSERT INTO events (event_type, payload, idempotency_key)
VALUES (
    'invoice.paid',
    '{"invoice_id":"inv_123","amount":500}'::jsonb,
    'invoice-inv_123-paid-v1'
)
RETURNING id AS new_id \gset

INSERT INTO deliveries (
    event_id,
    endpoint_id
)
SELECT
    :new_id,
    id
FROM endpoints
WHERE disabled_at IS NULL
RETURNING id, event_id, endpoint_id, status;

ROLLBACK;
