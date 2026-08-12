package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Ian747-tw/relayforge/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) &&
		pgErr.Code == "23505"
}

func ValidateCreateEventParams(params domain.CreateEventParams) error {
	if strings.TrimSpace(params.EventType) == "" {
		return errors.New("event type is required")
	}

	if strings.TrimSpace(params.IdempotencyKey) == "" {
		return errors.New("idempotency key is required")
	}

	if !json.Valid(params.Payload) {
		return errors.New("payload must be valid JSON")
	}

	if len(params.EndpointIDs) == 0 {
		return errors.New("at least one endpoint is required")
	}

	seen := make(map[int64]struct{}, len(params.EndpointIDs))

	for _, endpointID := range params.EndpointIDs {
		if endpointID <= 0 {
			return fmt.Errorf(
				"endpoint ID must be positive: %d",
				endpointID,
			)
		}

		if _, exists := seen[endpointID]; exists {
			return fmt.Errorf(
				"duplicate endpoint ID: %d",
				endpointID,
			)
		}

		seen[endpointID] = struct{}{}
	}

	return nil
}

func (s *Store) CreateEventWithDeliveries(
	ctx context.Context,
	params domain.CreateEventParams,
) (event domain.Event, created bool, err error) {
	requestHash, err := domain.RequestHash(params)
	if err != nil {
		return domain.Event{}, false, fmt.Errorf(
			"compute request hash: %w",
			err,
		)
	}

	if err := ValidateCreateEventParams(params); err != nil {
		return domain.Event{}, false, fmt.Errorf("validate event: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Event{}, false, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var new_event domain.Event

	err = tx.QueryRow(
		ctx,
		`
		INSERT INTO events (event_type, payload, idempotency_key, request_hash)
		VALUES (
		    $1, $2, $3, $4
		)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id, event_type, created_at, payload, idempotency_key
		`,
		params.EventType,
		params.Payload,
		params.IdempotencyKey,
		requestHash,
	).Scan(
		&new_event.ID,
		&new_event.EventType,
		&new_event.CreatedAt,
		&new_event.Payload,
		&new_event.IdempotencyKey,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			var existingRequestHash []byte
			var existing domain.Event
			er := tx.QueryRow(
				ctx,
				`
				SELECT
				    id,
				    event_type,
				    created_at,
				    payload,
				    idempotency_key,
				    request_hash
				FROM events
				WHERE idempotency_key = $1;
				`,
				params.IdempotencyKey,
			).Scan(
				&existing.ID,
				&existing.EventType,
				&existing.CreatedAt,
				&existing.Payload,
				&existing.IdempotencyKey,
				&existingRequestHash,
			)

			if er != nil {
				return domain.Event{}, false, fmt.Errorf("error fetching existing event: %w", er)
			}

			if !bytes.Equal(existingRequestHash, requestHash) {
				return domain.Event{}, false, domain.ErrIdempotencyConflict
			}

			if err := tx.Commit(ctx); err != nil {
				return domain.Event{}, false, fmt.Errorf(
					"commit replay transaction: %w",
					err,
				)
			}

			return existing, false, nil

		}
		return domain.Event{}, false, fmt.Errorf("insert event: %w", err)
	}

	if err := verifyActiveEndpoints(
		ctx,
		tx,
		params.EndpointIDs,
	); err != nil {
		return domain.Event{}, false, fmt.Errorf("verify endpoints: %w", err)
	}

	for _, endpoint_id := range params.EndpointIDs {
		_, err := tx.Exec(
			ctx,
			`
			INSERT INTO deliveries (event_id, endpoint_id)
			VALUES (
				$1, $2
			)
			`,
			new_event.ID,
			endpoint_id,
		)
		if err != nil {
			return domain.Event{}, false, fmt.Errorf("insert delivery: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.Event{}, false, fmt.Errorf("commit transaction: %w", err)
	}

	return new_event, true, nil

}

func (s *Store) GetEvent(
	ctx context.Context,
	id int64,
) (domain.Event, error) {
	var event domain.Event

	err := s.pool.QueryRow(
		ctx,
		`
		SELECT
			id,
			event_type,
			payload,
			idempotency_key,
			created_at
		FROM events
		WHERE id = $1
		`,
		id,
	).Scan(
		&event.ID,
		&event.EventType,
		&event.Payload,
		&event.IdempotencyKey,
		&event.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Event{}, domain.ErrNotFound
		}
		return domain.Event{}, fmt.Errorf("unexpected fetching event: %w", err)
	}

	return event, nil
}
