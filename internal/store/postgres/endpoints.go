package postgres

import (
	"context"
	"fmt"

	"github.com/Ian747-tw/relayforge/internal/domain"
	"github.com/jackc/pgx/v5"
)

func (s *Store) CreateEndpoint(
	ctx context.Context,
	url string,
	secret string,
) (domain.Endpoint, error) {
	var endpoint domain.Endpoint

	err := s.pool.QueryRow(
		ctx,
		`
		INSERT INTO endpoints (url, secret)
		VALUES ($1, $2)
		RETURNING id, url, secret, created_at, disabled_at
		`,
		url,
		secret,
	).Scan(
		&endpoint.ID,
		&endpoint.URL,
		&endpoint.Secret,
		&endpoint.CreatedAt,
		&endpoint.DisabledAt,
	)

	if err != nil {
		return domain.Endpoint{}, fmt.Errorf("create endpoint: %w", err)
	}

	return endpoint, nil

}

func (s *Store) ListActiveEndpoints(
	ctx context.Context,
) ([]domain.Endpoint, error) {
	rows, err := s.pool.Query(
		ctx,
		`
		SELECT id, url, secret, created_at, disabled_at
		FROM endpoints
		WHERE disabled_at IS NULL
		ORDER BY id
		`,
	)

	if err != nil {
		return []domain.Endpoint{}, fmt.Errorf("list active endpoints: %w", err)
	}

	defer rows.Close()

	var endpoints []domain.Endpoint

	for rows.Next() {
		var e domain.Endpoint
		err := rows.Scan(&e.ID, &e.URL, &e.Secret, &e.CreatedAt, &e.DisabledAt)
		if err != nil {
			return []domain.Endpoint{}, fmt.Errorf("list active endpoints: %w", err)
		}
		endpoints = append(endpoints, e)
	}

	if rows.Err() != nil {
		return []domain.Endpoint{}, fmt.Errorf("list active endpoints: %w", err)
	}

	return endpoints, nil
}

func (s *Store) DisableEndpoint(ctx context.Context, endpointID int64) error {
	var id int64

	err := s.pool.QueryRow(
		ctx,
		`
		UPDATE endpoints
		SET disabled_at = COALESCE(disabled_at, now())
		WHERE id = $1
		RETURNING id
		`,
		endpointID,
	).Scan(&id)

	if err != nil {
		return fmt.Errorf("disable endpoint: %w", err)
	}

	return nil
}

func verifyActiveEndpoints(
	ctx context.Context,
	tx pgx.Tx,
	endpointIDs []int64,
) error {
	var activeCount int64

	err := tx.QueryRow(
		ctx,
		`
		SELECT count(*)
		FROM (
			SELECT id
			FROM endpoints
			WHERE id = ANY($1::bigint[])
				AND disabled_at IS NULL
			FOR SHARE
		) AS locked_endpoints
		`,
		endpointIDs,
	).Scan(&activeCount)

	if err != nil {
		return fmt.Errorf("count active endpoints: %w", err)
	}

	if activeCount != int64(len(endpointIDs)) {
		return domain.ErrEndpointUnavailable
	}

	return nil

}
