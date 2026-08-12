package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Ian747-tw/relayforge/internal/domain"
	"github.com/jackc/pgx/v5"
)

func (s *Store) GetDelivery(ctx context.Context, id int64) (domain.DeliveryDetails, error) {
	var d domain.DeliveryDetails

	err := s.pool.QueryRow(
		ctx,
		`
		SELECT
		    d.id,
		    d.event_id,
		    d.endpoint_id,
		    d.status,
		    d.attempts_count,
		    d.next_attempt_due,
		    d.created_at,
		    e.event_type,
		    e.payload,
		    ep.url
		FROM deliveries AS d
		JOIN events AS e
		    ON e.id = d.event_id
		JOIN endpoints AS ep
		    ON ep.id = d.endpoint_id
		WHERE d.id = $1
		`,
		id,
	).Scan(&d.ID, &d.EventID, &d.EndpointID, &d.Status, &d.AttemptsCount, &d.NextAttemptDue, &d.CreatedAt, &d.EventType, &d.Payload, &d.EndpointURL)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.DeliveryDetails{}, domain.ErrNotFound
		}
		return domain.DeliveryDetails{}, fmt.Errorf("fetcting deliveries: %w", err)
	}

	return d, nil
}

func (s *Store) ListDueDeliveries(ctx context.Context, limit int) ([]domain.Delivery, error) {
	rows, err := s.pool.Query(
		ctx,
		`
		SELECT
		    id,
		    event_id,
		    endpoint_id,
		    status,
		    attempts_count,
		    next_attempt_due,
		    created_at,
		    completed_at
		FROM deliveries
		WHERE status IN ('pending', 'retry_scheduled')
			AND next_attempt_due <= now()
		ORDER BY next_attempt_due, id
		LIMIT $1;
		`,
		limit,
	)

	if err != nil {
		return []domain.Delivery{}, fmt.Errorf("fetching due deliveries: %w", err)
	}

	defer rows.Close()

	var fetched_data []domain.Delivery
	for rows.Next() {
		var d domain.Delivery
		err := rows.Scan(&d.ID, &d.EventID, &d.EndpointID, &d.Status, &d.AttemptsCount, &d.NextAttemptDue, &d.CreatedAt, &d.CompletedAt)
		if err != nil {
			return []domain.Delivery{}, fmt.Errorf("scanning rows: %w", err)
		}
		fetched_data = append(fetched_data, d)
	}

	if err := rows.Err(); err != nil {
		return []domain.Delivery{}, fmt.Errorf("scanning rows: %w", err)
	}

	return fetched_data, nil
}
