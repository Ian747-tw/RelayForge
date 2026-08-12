ALTER TABLE events
ADD COLUMN request_hash BYTEA;

-- Existing development events cannot be reconstructed reliably from
-- their original HTTP requests. Give them a sentinel hash so that
-- reusing their keys produces a conflict rather than an incorrect replay.
UPDATE events
SET request_hash = decode(repeat('00', 32), 'hex')
WHERE request_hash IS NULL;

ALTER TABLE events
ALTER COLUMN request_hash SET NOT NULL;

ALTER TABLE events
ADD CONSTRAINT events_request_hash_length
CHECK (octet_length(request_hash) = 32);
