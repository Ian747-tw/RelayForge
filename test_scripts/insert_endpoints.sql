\set ON_ERROR_STOP on

BEGIN;

INSERT INTO endpoints (url, secret)
VALUES
    ('example-1.com', 'secret-1'),
    ('example-2.com', 'secret-2')
RETURNING id, url, created_at;

SELECT id, url, created_at
FROM endpoints
WHERE disabled_at IS NULL
ORDER BY id;



ROLLBACK;
