package postgres

import (
	"context"
	"fmt"

	"github.com/Ian747-tw/relayforge/internal/domain"
)

func (s *Store) CreateAttempt(
	ctx context.Context,
	delivery_id int64,
	attempt_number int,
) (domain.DeliveryAttempt, error) {
	var attempt domain.DeliveryAttempt

	err := s.pool.QueryRow(
		ctx,
		`
		INSERT INTO delivery_attempts (delivery_id, attempt_number)
		VALUES ($1, $2)
		RETURNING *
		`,
		delivery_id,
		attempt_number,
	).Scan(
		&attempt.ID,
		&attempt.DeliveryID,
		&attempt.AttemptNumber,
		&attempt.ResponseStatus,
		&attempt.ErrorMessage,
		&attempt.StartedAt,
		&attempt.CompletedAt,
		&attempt.ResponseDurationMS,
	)

	if err != nil {
		return domain.DeliveryAttempt{}, fmt.Errorf("create attempt: %w", err)
	}

	return attempt, nil

}

func (s *Store) ListDeliveryAttempts(
	ctx context.Context,
	deliveryID int64,
) ([]domain.DeliveryAttempt, error) {
	var exists bool

	err := s.pool.QueryRow(
		ctx,
		`
		SELECT EXISTS (
			SELECT 1
			FROM deliveries
			WHERE id = $1
		)
		`,
		deliveryID,
	).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf(
			"check delivery existence: %w",
			err,
		)
	}

	if !exists {
		return nil, domain.ErrNotFound
	}

	rows, err := s.pool.Query(
		ctx,
		`
		SELECT
			id,
			delivery_id,
			attempt_number,
			started_at,
			completed_at,
			response_status,
			error_message,
			response_duration_ms
		FROM delivery_attempts
		WHERE delivery_id = $1
		ORDER BY attempt_number ASC
		`,
		deliveryID,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"query delivery attempts: %w",
			err,
		)
	}
	defer rows.Close()
	attempts := make([]domain.DeliveryAttempt, 0)

	for rows.Next() {
		var attempt domain.DeliveryAttempt

		if err := rows.Scan(
			&attempt.ID,
			&attempt.DeliveryID,
			&attempt.AttemptNumber,
			&attempt.StartedAt,
			&attempt.CompletedAt,
			&attempt.ResponseStatus,
			&attempt.ErrorMessage,
			&attempt.ResponseDurationMS,
		); err != nil {
			return nil, fmt.Errorf(
				"scan delivery attempt: %w",
				err,
			)
		}

		attempts = append(attempts, attempt)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate delivery attempts: %w",
			err,
		)
	}

	return attempts, nil
}
