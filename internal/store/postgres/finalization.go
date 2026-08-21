package postgres

import (
	"context"
	"fmt"

	"github.com/Ian747-tw/relayforge/internal/domain"
)

func (s *Store) FinalizeDeliveryAttempt(
	ctx context.Context,
	params domain.FinalizeDeliveryParams,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin finalization transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO delivery_attempts (
			delivery_id,
			attempt_number,
			started_at,
			completed_at,
			response_status,
			error_message,
			response_duration_ms
		)
		VALUES ($1, $2, $3, $4::timestamptz, $5, $6, $7)
		`,
		params.DeliveryID, params.AttemptNumber, params.StartedAt, params.CompletedAt, params.ResponseStatus, params.ErrorMessage, params.ResponseDurationMS,
	)
	if err != nil {
		return fmt.Errorf("insert into delivery attempts: %w", err)
	}

	tag, err := tx.Exec(
		ctx,
		`
		UPDATE deliveries
		SET
			status = $2,
			attempts_count = $3,
			claimed_at = NULL,
			completed_at = CASE
				WHEN $2 IN ('success', 'dead') THEN $4::timestamptz
				ELSE NULL::timestamptz
			END,
			next_attempt_due = CASE
				WHEN $2 = 'retry_scheduled' THEN $5::timestamptz
				ELSE next_attempt_due
			END
		WHERE id = $1 AND status = 'processing'
		`,
		params.DeliveryID, params.Status, params.AttemptNumber, params.CompletedAt, params.NextAttemptDUE,
	)
	if err != nil {
		return fmt.Errorf("update delivery result: %w", err)
	}

	if tag.RowsAffected() != 1 {
		return domain.ErrDeliveryNotProcessing
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit finalization transaction: %w", err)
	}

	return nil
}
