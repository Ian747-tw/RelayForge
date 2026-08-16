package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Ian747-tw/relayforge/internal/domain"
	"github.com/jackc/pgx/v5"
)

func (s *Store) ClaimDueDelivery(
	ctx context.Context,
) (domain.ClaimedDelivery, error) {
	const query = `
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
			ON ep.id = c.endpoint_id
	`

	var delivery domain.ClaimedDelivery

	err := s.pool.QueryRow(ctx, query).Scan(
		&delivery.ID,
		&delivery.EventID,
		&delivery.EndpointID,
		&delivery.AttemptCount,
		&delivery.ClaimedAt,
		&delivery.EventType,
		&delivery.Payload,
		&delivery.EndpointURL,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ClaimedDelivery{},
				domain.ErrNoDueDeliveries
		}

		return domain.ClaimedDelivery{},
			fmt.Errorf("claim due delivery: %w", err)
	}

	return delivery, nil
}
