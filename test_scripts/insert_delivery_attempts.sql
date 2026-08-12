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
RETURNING id AS new_event_id \gset

WITH inserted_deliveries AS (
    INSERT INTO deliveries (
        event_id,
        endpoint_id
    )
    SELECT
        :new_event_id,
        id
    FROM endpoints
    WHERE disabled_at IS NULL
    RETURNING id, event_id, endpoint_id, status
),
capture_one AS (
    SELECT id AS new_delivery_id
    FROM inserted_deliveries
    LIMIT 1
)

SELECT new_delivery_id FROM capture_one \gset

\echo '---start attempt---'

INSERT INTO delivery_attempts (delivery_id, attempt_number)
VALUES (:new_delivery_id, 1)
RETURNING *;

\echo '---processing---'

UPDATE deliveries
SET status = 'processing'
WHERE id = :new_delivery_id
RETURNING *;

\echo '---attempt_failed---'

UPDATE delivery_attempts
SET completed_at = now(), response_status = 403
WHERE delivery_id = :new_delivery_id AND attempt_number = 1
RETURNING *;

\echo '---retry delivery after fail---'

UPDATE deliveries
SET
    status = 'retry_scheduled',
    attempts_count = 1,
    next_attempt_due = now()
WHERE id = :new_delivery_id
RETURNING *;

\echo '---find due deliveries---'
SELECT *
FROM deliveries
WHERE status in ('pending', 'retry_scheduled')
    AND next_attempt_due <= now();

\echo '---record new attempt---'

INSERT INTO delivery_attempts (delivery_id, attempt_number)
VALUES (:new_delivery_id, 2)
RETURNING *;

\echo '---processing---'

UPDATE deliveries
SET status = 'processing'
WHERE id = :new_delivery_id
RETURNING *;

\echo '---retry succeed---'

UPDATE delivery_attempts
SET completed_at = now(), response_status = 202
WHERE delivery_id = :new_delivery_id AND attempt_number = 2
RETURNING *;

\echo 'delivery succeed'

UPDATE deliveries
SET status = 'success', completed_at = now(), attempts_count = 2
WHERE id = :new_delivery_id
RETURNING *;

ROLLBACK;
