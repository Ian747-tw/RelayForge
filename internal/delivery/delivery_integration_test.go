package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Ian747-tw/relayforge/internal/domain"
	"github.com/Ian747-tw/relayforge/internal/store/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

var testPool *pgxpool.Pool

func noJitter(max time.Duration) time.Duration {
	return 0
}

func TestMain(m *testing.M) {
	databaseURL := "postgresql://relayforge:relayforge_dev_password@localhost:5432/relayforge_test"

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"create test pool: %v\n",
			err,
		)
		os.Exit(1)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		fmt.Fprintf(
			os.Stderr,
			"ping test database: %v\n",
			err,
		)
		os.Exit(1)
	}

	var databaseName string
	err = pool.QueryRow(
		ctx,
		"SELECT current_database()",
	).Scan(&databaseName)
	if err != nil {
		pool.Close()
		fmt.Fprintf(
			os.Stderr,
			"reading database name: %v\n",
			err,
		)
		os.Exit(1)
	}

	if databaseName != "relayforge_test" {
		pool.Close()
		fmt.Fprintf(
			os.Stderr,
			"refuse to test on %s, expected relayforge_test\n",
			databaseName,
		)
		os.Exit(1)
	}

	testPool = pool

	exitCode := m.Run()

	testPool.Close()

	os.Exit(exitCode)

}

func resetTestDatabase(t *testing.T) {
	t.Helper()

	_, err := testPool.Exec(
		context.Background(),
		`
		TRUNCATE TABLE
			delivery_attempts,
			deliveries,
			events,
			endpoints
		RESTART IDENTITY CASCADE
		`,
	)
	if err != nil {
		t.Fatalf("reset test database: %v", err)
	}
}

func TestDeliveryEndToEnd(t *testing.T) {
	t.Run("successful delivery", func(t *testing.T) {
		// 204 → succeeded
		resetTestDatabase(t)
		t.Cleanup(func() {
			resetTestDatabase(t)
		})

		ctx := context.Background()

		store := postgres.New(testPool)
		sender := NewSender(2 * time.Second)

		retryPolicy, err := NewRetryPolicy(
			30*time.Second,
			45*time.Minute,
			noJitter,
		)
		if err != nil {
			t.Fatalf("create retry policy: %v", err)
		}

		processor := NewProcessor(
			sender,
			store,
			8,
			retryPolicy,
		)

		type receivedRequest struct {
			Method      string
			ContentType string
			Body        []byte
		}

		requests := make(chan receivedRequest, 1)

		target := httptest.NewServer(
			http.HandlerFunc(func(
				w http.ResponseWriter,
				r *http.Request,
			) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read body: %v", err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				requests <- receivedRequest{
					Method:      r.Method,
					ContentType: r.Header.Get("Content-Type"),
					Body:        body,
				}

				w.WriteHeader(http.StatusNoContent)
			}),
		)
		defer target.Close()

		endpoint, err := store.CreateEndpoint(
			ctx,
			target.URL,
			"test-secret",
		)
		if err != nil {
			t.Fatalf("create endpoint: %v", err)
		}

		params := domain.CreateEventParams{
			EventType: "user.created",
			Payload:   json.RawMessage(`{"user_id":123}`),
			IdempotencyKey: fmt.Sprintf(
				"day7-success-%d",
				time.Now().UnixNano(),
			),
			EndpointIDs: []int64{endpoint.ID},
		}

		event, _, err := store.CreateEventWithDeliveries(ctx, params)
		if err != nil {
			t.Fatalf("create event: %v", err)
		}

		claimed, err := store.ClaimDueDelivery(ctx)
		if err != nil {
			t.Fatalf("claim delivery: %v", err)
		}

		if claimed.EventID != event.ID {
			t.Fatalf(
				"claimed EventID = %d, want %d",
				claimed.EventID,
				event.ID,
			)
		}

		if claimed.EndpointURL != target.URL {
			t.Errorf(
				"claimed EndpointURL = %q, want %q",
				claimed.EndpointURL,
				target.URL,
			)
		}

		if strings.TrimSpace(string(claimed.Payload)) != `{"user_id": 123}` {
			t.Errorf(
				"claimed payload = %s",
				claimed.Payload,
			)
		}

		err = processor.Process(ctx, claimed)
		if err != nil {
			t.Fatalf("process delivery: %v", err)
		}

		select {
		case request := <-requests:
			if request.Method != http.MethodPost {
				t.Errorf(
					"method = %s, want POST",
					request.Method,
				)
			}

			if request.ContentType != "application/json" {
				t.Errorf(
					"Content-Type = %q, want application/json",
					request.ContentType,
				)
			}

			if strings.TrimSpace(string(request.Body)) != `{"user_id": 123}` {
				t.Errorf(
					"body = %s, want %s",
					request.Body,
					`{"user_id":123}`,
				)
			}

		case <-time.After(time.Second):
			t.Fatal("target did not receive webhook")
		}

		var (
			status        domain.DeliveryStatus
			attemptsCount int
			claimedAt     *time.Time
			completedAt   *time.Time
		)

		err = testPool.QueryRow(
			ctx,
			`
			SELECT
				status,
				attempts_count,
				claimed_at,
				completed_at
			FROM deliveries
			WHERE id = $1
			`,
			claimed.ID,
		).Scan(
			&status,
			&attemptsCount,
			&claimedAt,
			&completedAt,
		)
		if err != nil {
			t.Fatalf("query completed delivery: %v", err)
		}

		if status != domain.DeliveryStatusSucceeded {
			t.Errorf(
				"status = %q, want %q",
				status,
				domain.DeliveryStatusSucceeded,
			)
		}

		if attemptsCount != 1 {
			t.Errorf(
				"attempts_count = %d, want 1",
				attemptsCount,
			)
		}

		if claimedAt != nil {
			t.Errorf("claimed_at = %v, want nil", claimedAt)
		}

		if completedAt == nil {
			t.Error("completed_at = nil, want non-nil")
		}

		var (
			attemptNumber  int
			responseStatus *int
			errorMessage   *string
		)

		err = testPool.QueryRow(
			ctx,
			`
			SELECT
				attempt_number,
				response_status,
				error_message
			FROM delivery_attempts
			WHERE delivery_id = $1
			`,
			claimed.ID,
		).Scan(
			&attemptNumber,
			&responseStatus,
			&errorMessage,
		)
		if err != nil {
			t.Fatalf("query delivery attempt: %v", err)
		}

		if attemptNumber != 1 {
			t.Errorf(
				"attempt_number = %d, want 1",
				attemptNumber,
			)
		}

		if responseStatus == nil || *responseStatus != http.StatusNoContent {
			t.Errorf(
				"response_status = %v, want 204",
				responseStatus,
			)
		}

		if errorMessage != nil {
			t.Errorf(
				"error_message = %q, want nil",
				*errorMessage,
			)
		}

	})

	t.Run("retryable delivery", func(t *testing.T) {
		// 500 → retry_scheduled
		resetTestDatabase(t)
		t.Cleanup(func() {
			resetTestDatabase(t)
		})

		ctx := context.Background()

		store := postgres.New(testPool)
		sender := NewSender(2 * time.Second)

		retryPolicy, err := NewRetryPolicy(
			30*time.Second,
			45*time.Minute,
			noJitter,
		)
		if err != nil {
			t.Fatalf("create retry policy: %v", err)
		}

		processor := NewProcessor(
			sender,
			store,
			8,
			retryPolicy,
		)

		type receivedRequest struct {
			Method      string
			ContentType string
			Body        []byte
		}

		requests := make(chan receivedRequest, 1)

		target := httptest.NewServer(
			http.HandlerFunc(func(
				w http.ResponseWriter,
				r *http.Request,
			) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read body: %v", err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				requests <- receivedRequest{
					Method:      r.Method,
					ContentType: r.Header.Get("Content-Type"),
					Body:        body,
				}

				w.WriteHeader(http.StatusInternalServerError)
			}),
		)
		defer target.Close()

		endpoint, err := store.CreateEndpoint(
			ctx,
			target.URL,
			"test-secret",
		)
		if err != nil {
			t.Fatalf("create endpoint: %v", err)
		}

		params := domain.CreateEventParams{
			EventType: "user.created",
			Payload:   json.RawMessage(`{"user_id":123}`),
			IdempotencyKey: fmt.Sprintf(
				"day7-success-%d",
				time.Now().UnixNano(),
			),
			EndpointIDs: []int64{endpoint.ID},
		}

		event, _, err := store.CreateEventWithDeliveries(ctx, params)
		if err != nil {
			t.Fatalf("create event: %v", err)
		}

		claimed, err := store.ClaimDueDelivery(ctx)
		if err != nil {
			t.Fatalf("claim delivery: %v", err)
		}

		if claimed.EventID != event.ID {
			t.Fatalf(
				"claimed EventID = %d, want %d",
				claimed.EventID,
				event.ID,
			)
		}

		if claimed.EndpointURL != target.URL {
			t.Errorf(
				"claimed EndpointURL = %q, want %q",
				claimed.EndpointURL,
				target.URL,
			)
		}

		if strings.TrimSpace(string(claimed.Payload)) != `{"user_id": 123}` {
			t.Errorf(
				"claimed payload = %s",
				claimed.Payload,
			)
		}

		err = processor.Process(ctx, claimed)
		if err != nil {
			t.Fatalf("process delivery: %v", err)
		}

		select {
		case request := <-requests:
			if request.Method != http.MethodPost {
				t.Errorf(
					"method = %s, want POST",
					request.Method,
				)
			}

			if request.ContentType != "application/json" {
				t.Errorf(
					"Content-Type = %q, want application/json",
					request.ContentType,
				)
			}

			if strings.TrimSpace(string(request.Body)) != `{"user_id": 123}` {
				t.Errorf(
					"body = %s, want %s",
					request.Body,
					`{"user_id":123}`,
				)
			}

		case <-time.After(time.Second):
			t.Fatal("target did not receive webhook")
		}

		_, err = store.ClaimDueDelivery(ctx)

		if !errors.Is(err, domain.ErrNoDueDeliveries) {
			t.Fatalf(
				"expected ErrNoDueDeliveries before retry time, got %v",
				err,
			)
		}

		var (
			status        domain.DeliveryStatus
			attemptsCount int
			claimedAt     *time.Time
			completedAt   *time.Time
		)

		err = testPool.QueryRow(
			ctx,
			`
			SELECT
				status,
				attempts_count,
				claimed_at,
				completed_at
			FROM deliveries
			WHERE id = $1
			`,
			claimed.ID,
		).Scan(
			&status,
			&attemptsCount,
			&claimedAt,
			&completedAt,
		)
		if err != nil {
			t.Fatalf("query completed delivery: %v", err)
		}

		if status != domain.DeliveryStatusRetryScheduled {
			t.Errorf(
				"status = %q, want %q",
				status,
				domain.DeliveryStatusRetryScheduled,
			)
		}

		if attemptsCount != 1 {
			t.Errorf(
				"attempts_count = %d, want 1",
				attemptsCount,
			)
		}

		if claimedAt != nil {
			t.Errorf("claimed_at = %v, want nil", claimedAt)
		}

		if completedAt != nil {
			t.Error("completed_at not nil, want nil")
		}

		var (
			attemptNumber  int
			responseStatus *int
			errorMessage   *string
		)

		err = testPool.QueryRow(
			ctx,
			`
			SELECT
				attempt_number,
				response_status,
				error_message
			FROM delivery_attempts
			WHERE delivery_id = $1
			`,
			claimed.ID,
		).Scan(
			&attemptNumber,
			&responseStatus,
			&errorMessage,
		)
		if err != nil {
			t.Fatalf("query delivery attempt: %v", err)
		}

		if attemptNumber != 1 {
			t.Errorf(
				"attempt_number = %d, want 1",
				attemptNumber,
			)
		}

		if responseStatus == nil || *responseStatus != http.StatusInternalServerError {
			t.Errorf(
				"response_status = %v, want 500",
				responseStatus,
			)
		}

		if errorMessage != nil {
			t.Errorf(
				"error_message = %q, want nil",
				*errorMessage,
			)
		}
	})

	t.Run("permanent failure", func(t *testing.T) {
		// 400 → dead
		resetTestDatabase(t)
		t.Cleanup(func() {
			resetTestDatabase(t)
		})

		ctx := context.Background()

		store := postgres.New(testPool)
		sender := NewSender(2 * time.Second)

		retryPolicy, err := NewRetryPolicy(
			1*time.Second,
			45*time.Minute,
			noJitter,
		)
		if err != nil {
			t.Fatalf("create retry policy: %v", err)
		}

		processor := NewProcessor(
			sender,
			store,
			8,
			retryPolicy,
		)

		type receivedRequest struct {
			Method      string
			ContentType string
			Body        []byte
		}

		requests := make(chan receivedRequest, 1)

		target := httptest.NewServer(
			http.HandlerFunc(func(
				w http.ResponseWriter,
				r *http.Request,
			) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read body: %v", err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				requests <- receivedRequest{
					Method:      r.Method,
					ContentType: r.Header.Get("Content-Type"),
					Body:        body,
				}

				w.WriteHeader(http.StatusBadRequest)
			}),
		)
		defer target.Close()

		endpoint, err := store.CreateEndpoint(
			ctx,
			target.URL,
			"test-secret",
		)
		if err != nil {
			t.Fatalf("create endpoint: %v", err)
		}

		params := domain.CreateEventParams{
			EventType: "user.created",
			Payload:   json.RawMessage(`{"user_id":123}`),
			IdempotencyKey: fmt.Sprintf(
				"day7-success-%d",
				time.Now().UnixNano(),
			),
			EndpointIDs: []int64{endpoint.ID},
		}

		event, _, err := store.CreateEventWithDeliveries(ctx, params)
		if err != nil {
			t.Fatalf("create event: %v", err)
		}

		claimed, err := store.ClaimDueDelivery(ctx)
		if err != nil {
			t.Fatalf("claim delivery: %v", err)
		}

		if claimed.EventID != event.ID {
			t.Fatalf(
				"claimed EventID = %d, want %d",
				claimed.EventID,
				event.ID,
			)
		}

		if claimed.EndpointURL != target.URL {
			t.Errorf(
				"claimed EndpointURL = %q, want %q",
				claimed.EndpointURL,
				target.URL,
			)
		}

		if strings.TrimSpace(string(claimed.Payload)) != `{"user_id": 123}` {
			t.Errorf(
				"claimed payload = %s",
				claimed.Payload,
			)
		}

		err = processor.Process(ctx, claimed)
		if err != nil {
			t.Fatalf("process delivery: %v", err)
		}

		select {
		case request := <-requests:
			if request.Method != http.MethodPost {
				t.Errorf(
					"method = %s, want POST",
					request.Method,
				)
			}

			if request.ContentType != "application/json" {
				t.Errorf(
					"Content-Type = %q, want application/json",
					request.ContentType,
				)
			}

			if strings.TrimSpace(string(request.Body)) != `{"user_id": 123}` {
				t.Errorf(
					"body = %s, want %s",
					request.Body,
					`{"user_id":123}`,
				)
			}

		case <-time.After(time.Second):
			t.Fatal("target did not receive webhook")
		}

		var (
			status        domain.DeliveryStatus
			attemptsCount int
			claimedAt     *time.Time
			completedAt   *time.Time
		)

		err = testPool.QueryRow(
			ctx,
			`
			SELECT
				status,
				attempts_count,
				claimed_at,
				completed_at
			FROM deliveries
			WHERE id = $1
			`,
			claimed.ID,
		).Scan(
			&status,
			&attemptsCount,
			&claimedAt,
			&completedAt,
		)
		if err != nil {
			t.Fatalf("query completed delivery: %v", err)
		}

		if status != domain.DeliveryStatusDead {
			t.Errorf(
				"status = %q, want %q",
				status,
				domain.DeliveryStatusRetryScheduled,
			)
		}

		if attemptsCount != 1 {
			t.Errorf(
				"attempts_count = %d, want 1",
				attemptsCount,
			)
		}

		if claimedAt != nil {
			t.Errorf("claimed_at = %v, want nil", claimedAt)
		}

		if completedAt == nil {
			t.Error("completed_at nil, want non-nil")
		}

		var (
			attemptNumber  int
			responseStatus *int
			errorMessage   *string
		)

		err = testPool.QueryRow(
			ctx,
			`
			SELECT
				attempt_number,
				response_status,
				error_message
			FROM delivery_attempts
			WHERE delivery_id = $1
			`,
			claimed.ID,
		).Scan(
			&attemptNumber,
			&responseStatus,
			&errorMessage,
		)
		if err != nil {
			t.Fatalf("query delivery attempt: %v", err)
		}

		if attemptNumber != 1 {
			t.Errorf(
				"attempt_number = %d, want 1",
				attemptNumber,
			)
		}

		if responseStatus == nil || *responseStatus != http.StatusBadRequest {
			t.Errorf(
				"response_status = %v, want 400",
				responseStatus,
			)
		}

		if errorMessage != nil {
			t.Errorf(
				"error_message = %q, want nil",
				*errorMessage,
			)
		}

		time.Sleep(2 * time.Second)

		_, err = store.ClaimDueDelivery(ctx)

		if !errors.Is(err, domain.ErrNoDueDeliveries) {
			t.Fatalf(
				"expected ErrNoDueDeliveries for dead delivery, got %v",
				err,
			)
		}

	})

	t.Run("multiple endpoints", func(t *testing.T) {

		resetTestDatabase(t)
		t.Cleanup(func() {
			resetTestDatabase(t)
		})

		ctx := context.Background()

		store := postgres.New(testPool)
		sender := NewSender(2 * time.Second)

		retryPolicy, err := NewRetryPolicy(
			1*time.Second,
			45*time.Minute,
			noJitter,
		)
		if err != nil {
			t.Fatalf("create retry policy: %v", err)
		}

		processor := NewProcessor(
			sender,
			store,
			8,
			retryPolicy,
		)

		type receivedRequest struct {
			Method      string
			ContentType string
			Body        []byte
		}

		requests := make(chan receivedRequest, 1)

		target1 := httptest.NewServer(
			http.HandlerFunc(func(
				w http.ResponseWriter,
				r *http.Request,
			) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read body: %v", err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				requests <- receivedRequest{
					Method:      r.Method,
					ContentType: r.Header.Get("Content-Type"),
					Body:        body,
				}

				w.WriteHeader(http.StatusNoContent)
			}),
		)
		defer target1.Close()

		target2 := httptest.NewServer(
			http.HandlerFunc(func(
				w http.ResponseWriter,
				r *http.Request,
			) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read body: %v", err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				requests <- receivedRequest{
					Method:      r.Method,
					ContentType: r.Header.Get("Content-Type"),
					Body:        body,
				}

				w.WriteHeader(http.StatusNoContent)
			}),
		)
		defer target2.Close()

		endpoint1, err := store.CreateEndpoint(
			ctx,
			target1.URL,
			"test-secret",
		)

		endpoint2, err := store.CreateEndpoint(
			ctx,
			target2.URL,
			"test-secret",
		)

		if err != nil {
			t.Fatalf("create endpoint: %v", err)
		}

		params := domain.CreateEventParams{
			EventType: "user.created",
			Payload:   json.RawMessage(`{"user_id":123}`),
			IdempotencyKey: fmt.Sprintf(
				"day7-success-%d",
				time.Now().UnixNano(),
			),
			EndpointIDs: []int64{endpoint1.ID, endpoint2.ID},
		}

		event, _, err := store.CreateEventWithDeliveries(ctx, params)
		if err != nil {
			t.Fatalf("create event: %v", err)
		}

		for range 2 {
			claimed, err := store.ClaimDueDelivery(ctx)
			if err != nil {
				t.Fatalf("claim delivery: %v", err)
			}

			if claimed.EventID != event.ID {
				t.Fatalf("event ID = %d, want %d", claimed.EventID, event.ID)
			}

			err = processor.Process(ctx, claimed)
			if err != nil {
				t.Fatalf("process delivery: %v", err)
			}

			select {
			case request := <-requests:
				if request.Method != http.MethodPost {
					t.Errorf(
						"method = %s, want POST",
						request.Method,
					)
				}

				if request.ContentType != "application/json" {
					t.Errorf(
						"Content-Type = %q, want application/json",
						request.ContentType,
					)
				}

			case <-time.After(time.Second):
				t.Fatal("target did not receive webhook")
			}
		}

		var (
			numDeliveries int
			numAttempts   int
		)

		err = testPool.QueryRow(
			ctx,
			`
			SELECT COUNT(*)
			FROM deliveries
			`,
		).Scan(
			&numDeliveries,
		)
		if err != nil {
			t.Fatalf("query completed delivery: %v", err)
		}
		if numDeliveries != 2 {
			t.Fatalf("num deliveries = %d, want 2", numDeliveries)
		}

		err = testPool.QueryRow(
			ctx,
			`
			SELECT COUNT(*)
			FROM delivery_attempts
			`,
		).Scan(
			&numAttempts,
		)
		if err != nil {
			t.Fatalf("query delivery attempt: %v", err)
		}

		if numAttempts != 2 {
			t.Fatalf("num attempts = %d, want 2", numAttempts)
		}

	})
}
