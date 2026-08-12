CREATE INDEX delivery_due_idx
ON deliveries (next_attempt_due, id)
WHERE status IN ('pending', 'retry_scheduled');
