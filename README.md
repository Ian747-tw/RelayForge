# RelayForge

RelayForge is a webhook delivery service written in Go and PostgreSQL. It accepts events, creates deliveries for registered endpoints, and processes them asynchronously with retry and recovery behavior.

## Current Features

- Idempotent event submission
- Transactional event and delivery creation
- Multi-endpoint fan-out
- Concurrent background workers
- Atomic delivery claiming
- Exponential backoff with jitter
- `Retry-After` handling
- Bounded retries and dead-letter states
- Stale-claim recovery
- Graceful shutdown

## Delivery Guarantee

RelayForge provides at-least-once delivery.

A destination may receive the same webhook more than once if the request succeeds but RelayForge stops before recording the successful result. Receivers should deduplicate requests.

Exactly-once delivery is not guaranteed.

## Architecture

Event submission:

1. A client submits an event with an idempotency key.
2. RelayForge stores the event and its endpoint deliveries in one transaction.
3. Workers atomically claim due deliveries from PostgreSQL.
4. Each worker sends the webhook and records the attempt.
5. Failed deliveries are retried according to the retry policy.
6. Deliveries exceeding the attempt limit enter a terminal dead state.
7. The recovery runner releases claims abandoned by interrupted workers.

## Verification

```bash
go test -p=1 ./... -count=1
go test -race -p=1 ./... -count=1