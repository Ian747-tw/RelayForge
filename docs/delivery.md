# Webhook delivery contract

RelayForge provides at least one webhook delivery.

A delivery may be sent more than once if the destination receives the request but RelayForge fails before recording the successful result.

RelayForge does not guarantee event ordering.

## Outcomes

A delivery attempt succeeds when the destination returns an HTTP status from 200 through 299.

The following failures are retryable:

- Network and connection errors
- Request timeouts
- HTTP 408
- HTTP 425
- HTTP 429
- HTTP 500 through 599

Most other HTTP 4xx responses are permanent failures.

HTTP 3xx responses are permanent failures because RelayForge does not automatically follow redirects for webhook delivery.

## Initial configuration

- Worker count: 1
- Delivery timeout: 10 seconds
- Poll interval: 1 second
- Maximum attempts: 8
- Ordering: not guaranteed
- Initial retry delay: fixed at 30 seconds during Week 4
- Exponential backoff and jitter: Week 5

|Status|Meaning|
| ---- | ---- |
|pending| Created but never claimed|
|processing|Currently owned by a worker|
|retry_scheduled|Previous attempt failed; wait until next_attempt_at|
|succeed|Destination returned 2xx: terminal|
|dead|Permanently failed or exhausted attempts: terminal|

## Allowed transitions
pending -----> processing

retry_scheduled -----> processing

processing -----> retry_scheduled

processing -----> succeed

processing -----> dead


## Specify field invariants
- attempt_count: Number of HTTP requests that have been completed and recorded.

- claimed_at: Not-NULL only at processing state, cleared when finalizing an attempt.

- next_attempt_due: Earliest time at which a pending or retry-scheduled delivery can be claimed. It is always Not-NULL. For processing and terminal deliveries, its stored value is ignored 

## Delivery Claim
A delivery is claimable when:

1. status = pending

OR

2. status = retry_scheduled
   AND next_attempt_at <= current time

## Transaction Boundaries
```
Transaction A:
    select and lock due delivery
    update it to processing
    commit

No transaction:
    send HTTP request

Transaction B:
    insert delivery_attempt
    update delivery state
    commit
```

## Claim ordering
```
ORDER BY d.next_attempt_at, d.id
```

## Delivery Eligibility

```
WHERE d.status IN ('pending', 'retry_scheduled')
  AND d.next_attempt_at <= now()
```

## Claim Query
```
WITH candidate AS (
    SELECT d.id
    FROM deliveries AS d
    WHERE d.status IN ('pending', 'retry_scheduled')
      AND d.next_attempt_due <= now()
    ORDER BY d.next_attempt_due, d.id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
),
claimed AS (
    UPDATE deliveries AS d
    SET
        status = 'processing',
        claimed_at = now()
    FROM candidate
    WHERE d.id = candidate.id
    RETURNING d.*
)
SELECT
    c.id,
    c.event_id,
    c.endpoint_id,
    c.attempts_count,
    c.claimed_at,
    e.event_type,
    e.payload,
    ep.url
FROM claimed AS c
JOIN events AS e
    ON e.id = c.event_id
JOIN endpoints AS ep
    ON ep.id = c.endpoint_id;
```
