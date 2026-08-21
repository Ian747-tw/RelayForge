package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Ian747-tw/relayforge/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var testPool *pgxpool.Pool

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

func createTestDeliveries(t *testing.T, store *Store, ctx context.Context, number int) {
	t.Helper()

	endpoints := []int64{}

	for i := 0; i < number; i++ {
		endpoint, err := store.CreateEndpoint(
			ctx,
			fmt.Sprintf("https://%d.test/webhook", i),
			fmt.Sprintf("secret-%d", i),
		)
		if err != nil {
			t.Fatalf("create endpoint: %v", err)
		}

		endpoints = append(endpoints, endpoint.ID)
	}

	params := domain.CreateEventParams{
		EventType:      "invoice.paid",
		Payload:        json.RawMessage(`{"invoice_id":"inv-123"}`),
		IdempotencyKey: "test-fanout-001",
		EndpointIDs:    endpoints,
	}

	_, created, err := store.CreateEventWithDeliveries(
		ctx,
		params,
	)
	if err != nil {
		t.Fatalf("Create event: %v", err)
	}
	if !created {
		t.Fatal("expect created = true")
	}

}

func assertAttempt(
	t *testing.T,
	ctx context.Context,
	deliveryID int64,
	wantAttemptNumber int,
	wantResponseStatus *int,
	wantErrorMessage *string,
	wantDurationMS int64,
) {
	t.Helper()

	var (
		attemptNumber  int
		responseStatus *int
		errorMessage   *string
		durationMS     int64
	)

	err := testPool.QueryRow(
		ctx,
		`
		SELECT
			attempt_number,
			response_status,
			error_message,
			response_duration_ms
		FROM delivery_attempts
		WHERE delivery_id = $1
		`,
		deliveryID,
	).Scan(
		&attemptNumber,
		&responseStatus,
		&errorMessage,
		&durationMS,
	)
	if err != nil {
		t.Fatalf("query delivery attempt: %v", err)
	}

	if attemptNumber != wantAttemptNumber {
		t.Errorf(
			"attempt_number = %d, want %d",
			attemptNumber,
			wantAttemptNumber,
		)
	}

	if !equalOptionalInt(responseStatus, wantResponseStatus) {
		t.Errorf(
			"response_status = %v, want %v",
			responseStatus,
			wantResponseStatus,
		)
	}

	if !equalOptionalString(errorMessage, wantErrorMessage) {
		t.Errorf(
			"error_message = %v, want %v",
			errorMessage,
			wantErrorMessage,
		)
	}

	if durationMS != wantDurationMS {
		t.Errorf(
			"response_duration_ms = %d, want %d",
			durationMS,
			wantDurationMS,
		)
	}
}

func equalOptionalInt(a, b *int) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	return *a == *b
}

func equalOptionalString(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	return *a == *b
}

func TestCreateEndpoint(t *testing.T) {
	resetTestDatabase(t)
	t.Cleanup(func() {
		resetTestDatabase(t)
	})

	ctx := context.Background()
	store := New(testPool)

	endpoint, err := store.CreateEndpoint(
		ctx,
		"https://create-endpoint.test/webhook",
		"test-secret",
	)
	if err != nil {
		t.Fatalf("CreateEndpoint returned error: %v", err)
	}

	if endpoint.ID == 0 {
		t.Error("expected generated endpoint ID, got 0")
	}

	if endpoint.URL != "https://create-endpoint.test/webhook" {
		t.Errorf(
			"expected URL %q, got %q",
			"https://create-endpoint.test/webhook",
			endpoint.URL,
		)
	}

	if endpoint.Secret != "test-secret" {
		t.Errorf(
			"expected secret %q, got %q",
			"test-secret",
			endpoint.Secret,
		)
	}

	if endpoint.CreatedAt.IsZero() {
		t.Error("expected nonzero CreatedAt")
	}

	if endpoint.DisabledAt != nil {
		t.Errorf(
			"expected DisabledAt to be nil, got %v",
			endpoint.DisabledAt,
		)
	}
}

func TestListActiveEndpoints(t *testing.T) {
	resetTestDatabase(t)
	t.Cleanup(func() {
		resetTestDatabase(t)
	})

	ctx := context.Background()
	store := New(testPool)

	_, err := store.CreateEndpoint(
		ctx,
		"https://a.test/webhook",
		"secret-a",
	)
	if err != nil {
		t.Fatalf("create endpoint A: %v", err)
	}

	_, err = store.CreateEndpoint(
		ctx,
		"https://b.test/webhook",
		"secret-b",
	)
	if err != nil {
		t.Fatalf("create endpoint B: %v", err)
	}

	endpoints, err := store.ListActiveEndpoints(ctx)
	if err != nil {
		t.Fatalf("ListActiveEndpoints returned error: %v", err)
	}

	if len(endpoints) != 2 {
		t.Fatalf(
			"expected 2 endpoints, got %d",
			len(endpoints),
		)
	}

	if endpoints[0].URL != "https://a.test/webhook" {
		t.Errorf(
			"expected first URL to be endpoint A, got %q",
			endpoints[0].URL,
		)
	}

	if endpoints[1].URL != "https://b.test/webhook" {
		t.Errorf(
			"expected second URL to be endpoint B, got %q",
			endpoints[1].URL,
		)
	}
}

func TestCreateEventDeliveriesAttemptFlow(t *testing.T) {
	resetTestDatabase(t)
	t.Cleanup(func() {
		resetTestDatabase(t)
	})

	ctx := context.Background()
	store := New(testPool)

	//Test: create endpoints
	endpointA, err := store.CreateEndpoint(
		ctx,
		"https://a.test/webhook",
		"secret-a",
	)
	if err != nil {
		t.Fatalf("create endpoint A: %v", err)
	}

	endpointB, err := store.CreateEndpoint(
		ctx,
		"https://b.test/webhook",
		"secret-b",
	)
	if err != nil {
		t.Fatalf("create endpoint B: %v", err)
	}

	//Test: create event with deliveries
	params := domain.CreateEventParams{
		EventType:      "invoice.paid",
		Payload:        json.RawMessage(`{"invoice_id":"inv-123"}`),
		IdempotencyKey: "test-fanout-001",
		EndpointIDs: []int64{
			endpointA.ID,
			endpointB.ID,
		},
	}

	event, created, err := store.CreateEventWithDeliveries(
		ctx,
		params,
	)
	if err != nil {
		t.Fatalf("Create event: %v", err)
	}
	if !created {
		t.Fatal("expect created = true")
	}

	if event.ID == 0 {
		t.Error("expected generated event ID, got 0")
	}

	//Test: get event
	e, err := store.GetEvent(ctx, event.ID)
	if err != nil {
		t.Fatalf("Get Event: %v", err)
	}

	if e.ID != event.ID {
		t.Fatal("wrong return event id")
	}

	//Test: get event: invalid ID returns domain.ErrNotFound
	var invalidEID int64 = 99999

	_, err = store.GetEvent(ctx, invalidEID)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected err not found error, got %v", err)
	}

	//Test: get delivery
	detail, err := store.GetDelivery(ctx, 1)
	if err != nil {
		t.Fatalf("Get deliverie: %v", err)
	}

	if detail.ID != 1 {
		t.Fatalf("expected detail ID 1, got %d", detail.ID)
	}

	if detail.EventID != event.ID {
		t.Error("delivery detailcontain incorrect event ID")
	}

	if detail.EndpointID != endpointA.ID {
		t.Errorf("expected first delivery's endpoint ID %d, got %d", endpointA.ID, detail.EndpointID)
	}

	//Test: get delivery: invalid id returns domain.ErrNotFound
	var invalidID int64 = 99999

	_, err = store.GetDelivery(ctx, invalidID)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected err not found error, got %v", err)
	}

	//Test: Claim Due delivery
	delivery, err := store.ClaimDueDelivery(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if delivery.ID != 1 {
		t.Fatalf("expected delivery id 1, got %d", delivery.ID)
	}

	if delivery.EndpointID != endpointA.ID {
		t.Fatalf("expected endpoint id %d, got %d", endpointA.ID, delivery.EndpointID)
	}

	if delivery.EventID != e.ID {
		t.Fatalf("expected event id %d, got %d", e.ID, delivery.EventID)
	}

	var (
		status    string
		claimedAt *time.Time
	)

	err = testPool.QueryRow(
		ctx,
		`
		SELECT status, claimed_at
		FROM deliveries
		WHERE id = $1
		`,
		delivery.ID,
	).Scan(&status, &claimedAt)
	if err != nil {
		t.Fatalf("inspect claimed delivery: %v", err)
	}

	if status != "processing" {
		t.Errorf("status = %q, want processing", status)
	}

	if claimedAt == nil {
		t.Error("claimed_at is NULL, want non-NULL")
	}

	//Test future retries
	_, err = testPool.Exec(
		ctx,
		`
		UPDATE deliveries
		SET
			status = 'retry_scheduled',
			next_attempt_due = now() + interval '1 hour'
		WHERE id = 2
		`,
	)

	_, err = store.ClaimDueDelivery(ctx)
	if !errors.Is(err, domain.ErrNoDueDeliveries) {
		t.Fatalf("expected ErrNoDueDeliveries, got %v", err)
	}

	_, err = testPool.Exec(
		ctx,
		`
		UPDATE deliveries
		SET
			status = 'retry_scheduled',
			next_attempt_due = now() - interval '1 hour'
		WHERE id = 2
		`,
	)

	delivery2, err := store.ClaimDueDelivery(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if delivery2.ID != 2 {
		t.Fatalf("expected delivery id 2, got %d", delivery2.ID)
	}

	//Test: create attempt
	delivery_id := detail.ID

	attempt1, err := store.CreateAttempt(ctx, delivery_id, 1)
	if err != nil {
		t.Fatalf("create attempt 1: %v", err)
	}

	attempt2, err := store.CreateAttempt(ctx, delivery_id, 2)
	if err != nil {
		t.Fatalf("create attempt 2: %v", err)
	}

	//Test: list delivery attempts
	delivery_attempts, err := store.ListDeliveryAttempts(ctx, delivery_id)
	if err != nil {
		t.Fatalf("List delivery attempts: %v", err)
	}

	if len(delivery_attempts) != 2 {
		t.Fatalf("expected returning 2 attempts, got %d", len(delivery_attempts))
	}

	if delivery_attempts[0].DeliveryID != delivery_id {
		t.Errorf("expected attempt's delivery ID %d, got %d", delivery_id, delivery_attempts[0].DeliveryID)
	}

	if delivery_attempts[0].ID != attempt1.ID {
		t.Errorf("expected first attempt ID %d, got %d", attempt1.ID, delivery_attempts[0].ID)
	}

	if delivery_attempts[1].ID != attempt2.ID {
		t.Errorf("expected first attempt ID %d, got %d", attempt2.ID, delivery_attempts[1].ID)
	}

	//Test: list delivery attempts: existing delivery with no attempts return no error with empty slice
	delivery_attempts2, err := store.ListDeliveryAttempts(ctx, 2)
	if err != nil {
		t.Errorf("expected No error, got %v", err)
	}

	if len(delivery_attempts2) != 0 {
		t.Error("expected to receive empty slice")
	}

	//Test: list delivery attempts: existing delivery with no attempts return no error with empty slice
	_, err = store.ListDeliveryAttempts(ctx, 99999)
	if err == nil {
		t.Error("expected not found error, got nil")
	}

	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("expected not found error, got %v", err)
	}

}

func TestCreateEventWithDeliveriesRejectsUnavailableEndpoint(t *testing.T) {
	resetTestDatabase(t)
	t.Cleanup(func() {
		resetTestDatabase(t)
	})

	ctx := context.Background()
	store := New(testPool)

	endpoint, err := store.CreateEndpoint(
		ctx,
		"https://rollback.test/webhook",
		"test-secret",
	)
	if err != nil {
		t.Fatalf("create valid endpoint: %v", err)
	}

	const invalidEndpointID int64 = 999999999
	const idempotencyKey = "test-rollback-001"

	params := domain.CreateEventParams{
		EventType:      "transaction.rollback",
		Payload:        json.RawMessage(`{"test":true}`),
		IdempotencyKey: idempotencyKey,
		EndpointIDs: []int64{
			endpoint.ID,
			invalidEndpointID,
		},
	}

	_, created, err := store.CreateEventWithDeliveries(ctx, params)
	if created {
		t.Fatal("expect not created")
	}
	if err == nil {
		t.Fatal("expected endpoint validation error, but operation succeed")
	}

	if !errors.Is(err, domain.ErrEndpointUnavailable) {
		t.Fatalf("expected err endpoint error, got %T: %v", err, err)
	}

	var eventCount int
	err = testPool.QueryRow(
		ctx,
		`
		SELECT count(*)
		FROM events
		WHERE idempotency_key = $1
		`,
		idempotencyKey,
	).Scan(&eventCount)
	if err != nil {
		t.Fatalf("count rollback-test events: %v", err)
	}

	if eventCount != 0 {
		t.Errorf(
			"expected transaction to retain 0 events, got %d",
			eventCount,
		)
	}

	var deliveryCount int
	err = testPool.QueryRow(
		ctx,
		`
		SELECT count(*)
		FROM deliveries
		`,
	).Scan(&deliveryCount)
	if err != nil {
		t.Fatalf("count rollback-test deliveries: %v", err)
	}

	if deliveryCount != 0 {
		t.Errorf(
			"expected transaction to retain 0 deliveries, got %d",
			deliveryCount,
		)
	}

}

func TestCreateEventDuplicateIdempotencyKeyBehaviour(
	t *testing.T,
) {
	resetTestDatabase(t)
	t.Cleanup(func() {
		resetTestDatabase(t)
	})

	ctx := context.Background()
	store := New(testPool)

	endpoint, err := store.CreateEndpoint(
		ctx,
		"https://idempotency.test/webhook",
		"test-secret",
	)
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}

	params := domain.CreateEventParams{
		EventType:      "invoice.paid",
		Payload:        json.RawMessage(`{"invoice_id":"inv-123"}`),
		IdempotencyKey: "duplicate-key-001",
		EndpointIDs:    []int64{endpoint.ID},
	}

	params.Payload = json.RawMessage(`{"invoice_id" : "inv-123"}`)

	event, created, err := store.CreateEventWithDeliveries(ctx, params)
	if err != nil {
		t.Fatalf("first event creation failed: %v", err)
	}
	if !created {
		t.Fatal("expect created = true")
	}

	existing, created, err := store.CreateEventWithDeliveries(ctx, params)
	if err != nil {
		t.Fatalf(
			"expected existing duplicate-key succeed, got %v",
			err,
		)
	}
	if created {
		t.Fatal("expect created = false due to existing duplicate-key")
	}

	if event.ID != existing.ID {
		t.Fatal("expected event and existing ID the same due to valid duplicate-key")
	}

	params.EventType = "invoice.paid.invalid.duplicate"

	_, created, err = store.CreateEventWithDeliveries(ctx, params)
	if !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatal("expected duplicate-key error")
	}

	if created {
		t.Fatal("expected created = false")
	}

	var eventCount int
	err = testPool.QueryRow(
		ctx,
		`
		SELECT count(*)
		FROM events
		WHERE idempotency_key = $1
		`,
		params.IdempotencyKey,
	).Scan(&eventCount)
	if err != nil {
		t.Fatalf("count events: %v", err)
	}

	if eventCount != 1 {
		t.Errorf(
			"expected exactly 1 event after duplicate attempt, got %d",
			eventCount,
		)
	}
}

func TestListDueDeliveries(t *testing.T) {
	resetTestDatabase(t)
	t.Cleanup(func() {
		resetTestDatabase(t)
	})

	ctx := context.Background()
	store := New(testPool)

	endpoint, err := store.CreateEndpoint(
		ctx,
		"https://due_delivery.test/webhook",
		"test-secret",
	)
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}

	_, err = testPool.Exec(
		ctx,
		`
		INSERT INTO events (
    		event_type,
      		payload,
        	idempotency_key,
         	request_hash
		)
		SELECT
    		'index.test',
      		jsonb_build_object('sequence', n),
        	'index-test-' || n,
         	decode(repeat('00', 32), 'hex')
		FROM generate_series(1, 50000) AS n;
		`,
	)

	if err != nil {
		t.Fatalf("create events: %v", err)
	}

	_, err = testPool.Exec(
		ctx,
		`
		INSERT INTO deliveries (
		    event_id,
		    endpoint_id,
		    status,
		    attempts_count,
		    next_attempt_due,
		    completed_at
		)
		SELECT
		    id,
		    $1,
		    CASE
		        WHEN id % 10 = 0 THEN 'pending'
		        WHEN id % 10 = 1 THEN 'retry_scheduled'
		        ELSE 'success'
		    END,
		    CASE
		        WHEN id % 10 = 1 THEN 1
		        ELSE 0
		    END,
		    CASE
		        WHEN id % 10 IN (0, 1)
		            THEN now() - ((id % 3600) * interval '1 second')
		        ELSE now()
		    END,
		    CASE
		        WHEN id % 10 NOT IN (0, 1) THEN now()
		        ELSE NULL
		    END
				FROM events
				WHERE idempotency_key LIKE 'index-test-%';
		`,
		endpoint.ID,
	)

	if err != nil {
		t.Fatalf("create deliveries: %v", err)
	}

	due, err := store.ListDueDeliveries(ctx, 100)
	if err != nil {
		t.Fatalf("fetching due deliveries: %v", err)
	}

	if len(due) != 100 {
		t.Errorf("expected fetched deliveries to be 100, got %d", len(due))
	}

	var pre_next_attempt_due time.Time
	ready := false

	for _, d := range due {
		if d.Status != "pending" && d.Status != "retry_scheduled" {
			t.Fatalf("expected status to be pending or retry_scheduled, got %s", d.Status)
		}

		if d.NextAttemptDue.After(time.Now()) {
			t.Fatal("next attempt due is after now")
		}

		if ready && d.NextAttemptDue.Before(pre_next_attempt_due) {
			t.Fatal("next_attempt_due is not sorted")
		}
		ready = true
		pre_next_attempt_due = *d.NextAttemptDue
	}

}

func TestDisableEndpoints(t *testing.T) {
	resetTestDatabase(t)
	t.Cleanup(func() {
		resetTestDatabase(t)
	})

	ctx := context.Background()
	store := New(testPool)

	endpoint, err := store.CreateEndpoint(
		ctx,
		"https://disableEndpoint.test/webhook",
		"test-secret",
	)

	if err != nil {
		t.Fatalf("create valid endpoint: %v", err)
	}

	//test: disable endpoint
	err = store.DisableEndpoint(ctx, endpoint.ID)
	if err != nil {
		t.Fatalf("error disabling valid endpoint: %v", err)
	}

	endpoints, err := store.ListActiveEndpoints(ctx)
	if err != nil {
		t.Fatalf("ListActiveEndpoints returned error: %v", err)
	}

	if len(endpoints) != 0 {
		t.Fatalf("expected endpoint deleted, got %d", len(endpoints))
	}

	//test: disable twice
	err = store.DisableEndpoint(ctx, endpoint.ID)
	if err != nil {
		t.Fatalf("error disabling disabled endpoint: %v", err)
	}

	//test: disable missing id
	var invalidID int64 = 99999
	err = store.DisableEndpoint(ctx, invalidID)
	if err == nil {
		t.Fatal("expected err, got nil")
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("internal database error")
	}

}

func TestCreateEventWithDisabledEndpoint(t *testing.T) {
	resetTestDatabase(t)
	t.Cleanup(func() {
		resetTestDatabase(t)
	})

	ctx := context.Background()
	store := New(testPool)

	disabledEndpoint, err := store.CreateEndpoint(
		ctx,
		"https://DisabledEndpoint/webhook",
		"test-secret-1",
	)
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}

	activeEndpoint, err := store.CreateEndpoint(
		ctx,
		"https://ActiveEndpoint/webhook",
		"test-secret-2",
	)
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}

	err = store.DisableEndpoint(ctx, disabledEndpoint.ID)
	if err != nil {
		t.Fatalf("error disabling endpoint: %v", err)
	}

	const idempotencyKey = "test-event"

	params := domain.CreateEventParams{
		EventType:      "transaction.create",
		Payload:        json.RawMessage(`{"test":true}`),
		IdempotencyKey: idempotencyKey,
		EndpointIDs: []int64{
			disabledEndpoint.ID,
			activeEndpoint.ID,
		},
	}

	_, _, err = store.CreateEventWithDeliveries(ctx, params)
	if err == nil {
		t.Fatal("expect error creating event with disabled endpoint")
	}

	if !errors.Is(err, domain.ErrEndpointUnavailable) {
		t.Fatal("expect error to be endpoint unavailable")
	}
}

func TestCreateEventWithDeliveriesConcurrentReplay(t *testing.T) {
	resetTestDatabase(t)
	t.Cleanup(func() {
		resetTestDatabase(t)
	})

	ctx := context.Background()
	testStore := New(testPool)

	endpoint1, err := testStore.CreateEndpoint(
		ctx,
		"https://example.com/webhook-1",
		"secret-1",
	)
	if err != nil {
		t.Fatalf("create endpoint 1: %v", err)
	}

	endpoint2, err := testStore.CreateEndpoint(
		ctx,
		"https://example.com/webhook-2",
		"secret-2",
	)
	if err != nil {
		t.Fatalf("create endpoint 2: %v", err)
	}

	params := domain.CreateEventParams{
		EventType:      "user.created",
		Payload:        json.RawMessage(`{"user_id":123}`),
		IdempotencyKey: "concurrent-key-1",
		EndpointIDs:    []int64{endpoint1.ID, endpoint2.ID},
	}

	const workers = 5

	if testPool.Config().MaxConns < workers {
		t.Fatal("DB max conns < workers")
	}

	type result struct {
		event   domain.Event
		created bool
		err     error
	}

	start := make(chan struct{})
	results := make(chan result, workers)

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()

			// All goroutines wait here before sending the request.
			<-start

			event, created, err :=
				testStore.CreateEventWithDeliveries(ctx, params)

			results <- result{
				event:   event,
				created: created,
				err:     err,
			}
		}()
	}

	// Release every goroutine at approximately the same time.
	close(start)

	wg.Wait()
	close(results)

	var (
		createdCount int
		eventID      int64
	)

	for result := range results {
		if result.err != nil {
			t.Errorf("CreateEventWithDeliveries() error: %v", result.err)
			continue
		}

		if result.created {
			createdCount++
		}

		if eventID == 0 {
			eventID = result.event.ID
		} else if result.event.ID != eventID {
			t.Errorf(
				"requests returned different event IDs: got %d, want %d",
				result.event.ID,
				eventID,
			)
		}
	}

	if createdCount != 1 {
		t.Errorf(
			"created count = %d, want exactly 1",
			createdCount,
		)
	}

	if eventID == 0 {
		t.Fatal("no event was returned successfully")
	}

	var eventCount int

	err = testPool.QueryRow(
		ctx,
		`
		SELECT count(*)
		FROM events
		WHERE idempotency_key = $1
		`,
		params.IdempotencyKey,
	).Scan(&eventCount)
	if err != nil {
		t.Fatalf("count events: %v", err)
	}

	if eventCount != 1 {
		t.Errorf("event count = %d, want 1", eventCount)
	}

	var deliveryCount int

	err = testPool.QueryRow(
		ctx,
		`
		SELECT count(*)
		FROM deliveries
		WHERE event_id = $1
		`,
		eventID,
	).Scan(&deliveryCount)
	if err != nil {
		t.Fatalf("count deliveries: %v", err)
	}

	if deliveryCount != len(params.EndpointIDs) {
		t.Errorf(
			"delivery count = %d, want %d",
			deliveryCount,
			len(params.EndpointIDs),
		)
	}
}

func TestClaimDueDeliveryEmptyQueue(t *testing.T) {
	resetTestDatabase(t)
	t.Cleanup(func() {
		resetTestDatabase(t)
	})

	ctx := context.Background()
	store := New(testPool)

	_, err := store.ClaimDueDelivery(ctx)
	if !errors.Is(err, domain.ErrNoDueDeliveries) {
		t.Fatalf("expected NoDueDeliveries error, got %v", err)
	}
}

func TestTerminalStateNotClaimed(t *testing.T) {
	resetTestDatabase(t)
	t.Cleanup(func() {
		resetTestDatabase(t)
	})

	ctx := context.Background()
	store := New(testPool)

	createTestDeliveries(t, store, ctx, 1)

	tests := []struct {
		name   string
		status string
	}{
		{name: "procesing", status: "processing"},
		{name: "succeeded", status: "success"},
		{name: "dead", status: "dead"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.status == "processing" {
				_, err := testPool.Exec(
					ctx,
					`
					UPDATE deliveries
					SET status = $1
					WHERE id = 1
					`,
					tt.status,
				)
				if err != nil {
					t.Fatalf("error updating status: %v", err)
				}
			} else {
				_, err := testPool.Exec(
					ctx,
					`
					UPDATE deliveries
					SET status = $1, completed_at = now()
					WHERE id = 1
					`,
					tt.status,
				)
				if err != nil {
					t.Fatalf("error updating status: %v", err)
				}
			}

			_, err := store.ClaimDueDelivery(ctx)
			if !errors.Is(err, domain.ErrNoDueDeliveries) {
				t.Fatalf("expected ErrNoDueDeliveries, got %v", err)
			}

		})
	}

}

func TestDeterministicOrdering(t *testing.T) {
	resetTestDatabase(t)
	t.Cleanup(func() {
		resetTestDatabase(t)
	})

	ctx := context.Background()
	store := New(testPool)

	createTestDeliveries(t, store, ctx, 2)

	_, err := testPool.Exec(
		ctx,
		`
		UPDATE deliveries
		SET status = 'retry_scheduled', next_attempt_due = now() - interval '1 minute'
		WHERE id = 1
		`,
	)
	if err != nil {
		t.Fatalf("error setting attempt due: %v", err)
	}

	_, err = testPool.Exec(
		ctx,
		`
		UPDATE deliveries
		SET status = 'retry_scheduled', next_attempt_due = now() - interval '2 minutes'
		WHERE id = 2
		`,
	)
	if err != nil {
		t.Fatalf("error setting attempt due: %v", err)
	}

	delivery, err := store.ClaimDueDelivery(ctx)
	if err != nil {
		t.Fatalf("claim due delivery error: %v", err)
	}

	if delivery.ID != 2 {
		t.Fatalf("expected delivery ID 2, got %d", delivery.ID)
	}

}

func TestClaimDueDeliveryConcurrent(t *testing.T) {
	resetTestDatabase(t)
	t.Cleanup(func() {
		resetTestDatabase(t)
	})

	ctx := context.Background()
	store := New(testPool)

	createTestDeliveries(t, store, ctx, 1)

	type result struct {
		id  int64
		err error
	}

	const workers = 5

	if testPool.Config().MaxConns < workers {
		t.Fatal("DB max conns < workers")
	}

	start := make(chan struct{})
	results := make(chan result, workers)

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			<-start
			d, err := store.ClaimDueDelivery(ctx)

			if err != nil {
				results <- result{
					id:  -1,
					err: err,
				}
			} else {
				results <- result{
					id:  d.ID,
					err: err,
				}
			}
		}()
	}

	close(start)

	wg.Wait()

	close(results)

	claimCount := 0
	notFoundCount := 0

	for res := range results {
		if res.err != nil {
			if !errors.Is(res.err, domain.ErrNoDueDeliveries) {
				t.Fatalf("unexpected error claiming: %v", res.err)
			}
			notFoundCount++
		} else {
			if res.id != 1 {
				t.Fatalf("expected claimed id 1, got %d", res.id)
			}
			claimCount++
		}
	}

	if claimCount != 1 {
		t.Fatalf("expected claiming 1, got %d", claimCount)
	}

	if notFoundCount+claimCount != workers {
		t.Fatalf("expected receiving %d results, got %d", workers, notFoundCount+claimCount)
	}
}

func TestClaimDueDeliveryConcurrentMuitipleRows(t *testing.T) {
	resetTestDatabase(t)
	t.Cleanup(func() {
		resetTestDatabase(t)
	})

	ctx := context.Background()
	store := New(testPool)

	const workers = 5
	if testPool.Config().MaxConns < workers {
		t.Fatal("DB max conns < workers")
	}

	createTestDeliveries(t, store, ctx, workers)

	type result struct {
		id  int64
		err error
	}

	start := make(chan struct{})
	results := make(chan result, workers)

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			<-start

			d, err := store.ClaimDueDelivery(ctx)
			if err != nil {
				results <- result{
					id:  -1,
					err: err,
				}
			} else {
				results <- result{
					id:  d.ID,
					err: err,
				}
			}

		}()
	}

	close(start)
	wg.Wait()
	close(results)

	distinctID := make(map[int64]struct{})

	for res := range results {
		if res.err != nil {
			t.Fatalf("expected no err, got %v", res.err)
		}

		distinctID[res.id] = struct{}{}
	}

	if len(distinctID) != workers {
		t.Fatalf("expected claiming %d distinct deliveries, got %d", workers, len(distinctID))
	}

}

func TestFinalizeDeliveryAttempt(t *testing.T) {
	resetTestDatabase(t)
	t.Cleanup(func() {
		resetTestDatabase(t)
	})

	ctx := context.Background()
	store := New(testPool)

	createTestDeliveries(t, store, ctx, 5)

	_, err := testPool.Exec(
		ctx,
		`
		UPDATE deliveries
		SET status = 'processing'
		`,
	)
	if err != nil {
		t.Fatalf("update deliveries to processing, %v", err)
	}

	t.Run("successful delivery", func(t *testing.T) {
		var deliveryID int64 = 1
		startedAt := time.Now().Add(-100 * time.Millisecond).UTC()
		completedAt := time.Now().UTC()
		responseStatus := http.StatusNoContent

		err := store.FinalizeDeliveryAttempt(
			ctx,
			domain.FinalizeDeliveryParams{
				DeliveryID:         deliveryID,
				AttemptNumber:      1,
				StartedAt:          startedAt,
				CompletedAt:        completedAt,
				ResponseStatus:     &responseStatus,
				ErrorMessage:       nil,
				ResponseDurationMS: 100,
				Status:             domain.DeliveryStatusSucceeded,
				NextAttemptDUE:     nil,
			},
		)
		if err != nil {
			t.Fatalf("FinalizeDeliveryAttempt() error = %v", err)
		}

		var (
			status       domain.DeliveryStatus
			attemptCount int
			claimedAt    *time.Time
			finishedAt   *time.Time
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
			deliveryID,
		).Scan(
			&status,
			&attemptCount,
			&claimedAt,
			&finishedAt,
		)
		if err != nil {
			t.Fatalf("query delivery: %v", err)
		}

		if status != domain.DeliveryStatusSucceeded {
			t.Errorf(
				"status = %q, want %q",
				status,
				domain.DeliveryStatusSucceeded,
			)
		}

		if attemptCount != 1 {
			t.Errorf("attempts_count = %d, want 1", attemptCount)
		}

		if claimedAt != nil {
			t.Errorf("claimed_at = %v, want nil", claimedAt)
		}

		if finishedAt == nil {
			t.Error("completed_at = nil, want non-nil")
		}

		assertAttempt(
			t,
			ctx,
			deliveryID,
			1,
			&responseStatus,
			nil,
			100,
		)
	})

	t.Run("retryable delivery", func(t *testing.T) {
		var deliveryID int64 = 2

		startedAt := time.Now().Add(-200 * time.Millisecond).UTC()
		completedAt := time.Now().UTC()
		nextAttemptAt := completedAt.Add(30 * time.Second)
		responseStatus := http.StatusInternalServerError

		err := store.FinalizeDeliveryAttempt(
			ctx,
			domain.FinalizeDeliveryParams{
				DeliveryID:         deliveryID,
				AttemptNumber:      1,
				StartedAt:          startedAt,
				CompletedAt:        completedAt,
				ResponseStatus:     &responseStatus,
				ErrorMessage:       nil,
				ResponseDurationMS: 200,
				Status:             domain.DeliveryStatusRetryScheduled,
				NextAttemptDUE:     &nextAttemptAt,
			},
		)
		if err != nil {
			t.Fatalf("FinalizeDeliveryAttempt() error = %v", err)
		}

		var (
			status          domain.DeliveryStatus
			attemptCount    int
			claimedAt       *time.Time
			storedNextAt    time.Time
			storedCompleted *time.Time
		)

		err = testPool.QueryRow(
			ctx,
			`
			SELECT
				status,
				attempts_count,
				claimed_at,
				next_attempt_due,
				completed_at
			FROM deliveries
			WHERE id = $1
			`,
			deliveryID,
		).Scan(
			&status,
			&attemptCount,
			&claimedAt,
			&storedNextAt,
			&storedCompleted,
		)
		if err != nil {
			t.Fatalf("query delivery: %v", err)
		}

		if status != domain.DeliveryStatusRetryScheduled {
			t.Errorf(
				"status = %q, want %q",
				status,
				domain.DeliveryStatusRetryScheduled,
			)
		}

		if attemptCount != 1 {
			t.Errorf("attempts_count = %d, want 1", attemptCount)
		}

		if claimedAt != nil {
			t.Errorf("claimed_at = %v, want nil", claimedAt)
		}

		if storedCompleted != nil {
			t.Errorf(
				"completed_at = %v, want nil",
				storedCompleted,
			)
		}

		assertAttempt(
			t,
			ctx,
			deliveryID,
			1,
			&responseStatus,
			nil,
			200,
		)
	})

	t.Run("network failure has null response status", func(t *testing.T) {
		var deliveryID int64 = 3

		startedAt := time.Now().Add(-150 * time.Millisecond).UTC()
		completedAt := time.Now().UTC()
		nextAttemptAt := completedAt.Add(30 * time.Second)
		errorMessage := "send webhook: connection refused"

		err := store.FinalizeDeliveryAttempt(
			ctx,
			domain.FinalizeDeliveryParams{
				DeliveryID:         deliveryID,
				AttemptNumber:      1,
				StartedAt:          startedAt,
				CompletedAt:        completedAt,
				ResponseStatus:     nil,
				ErrorMessage:       &errorMessage,
				ResponseDurationMS: 150,
				Status:             domain.DeliveryStatusRetryScheduled,
				NextAttemptDUE:     &nextAttemptAt,
			},
		)
		if err != nil {
			t.Fatalf("FinalizeDeliveryAttempt() error = %v", err)
		}

		assertAttempt(
			t,
			ctx,
			deliveryID,
			1,
			nil,
			&errorMessage,
			150,
		)
	})

	t.Run("permanent failure", func(t *testing.T) {
		var deliveryID int64 = 4

		startedAt := time.Now().Add(-50 * time.Millisecond).UTC()
		completedAt := time.Now().UTC()
		responseStatus := http.StatusBadRequest

		err := store.FinalizeDeliveryAttempt(
			ctx,
			domain.FinalizeDeliveryParams{
				DeliveryID:         deliveryID,
				AttemptNumber:      1,
				StartedAt:          startedAt,
				CompletedAt:        completedAt,
				ResponseStatus:     &responseStatus,
				ErrorMessage:       nil,
				ResponseDurationMS: 50,
				Status:             domain.DeliveryStatusDead,
				NextAttemptDUE:     nil,
			},
		)
		if err != nil {
			t.Fatalf("FinalizeDeliveryAttempt() error = %v", err)
		}

		var (
			status       domain.DeliveryStatus
			attemptCount int
			claimedAt    *time.Time
			completed    *time.Time
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
			deliveryID,
		).Scan(
			&status,
			&attemptCount,
			&claimedAt,
			&completed,
		)
		if err != nil {
			t.Fatalf("query delivery: %v", err)
		}

		if status != domain.DeliveryStatusDead {
			t.Errorf(
				"status = %q, want %q",
				status,
				domain.DeliveryStatusDead,
			)
		}

		if attemptCount != 1 {
			t.Errorf("attempts_count = %d, want 1", attemptCount)
		}

		if claimedAt != nil {
			t.Errorf("claimed_at = %v, want nil", claimedAt)
		}

		if completed == nil {
			t.Error("completed_at = nil, want non-nil")
		}

		assertAttempt(
			t,
			ctx,
			deliveryID,
			1,
			&responseStatus,
			nil,
			50,
		)
	})

	t.Run("non-processing delivery rolls back attempt", func(t *testing.T) {
		var deliveryID int64 = 5

		// Make the delivery invalid for finalization.
		_, err := testPool.Exec(
			ctx,
			`
			UPDATE deliveries
			SET
				status = 'pending',
				claimed_at = NULL
			WHERE id = $1
			`,
			deliveryID,
		)
		if err != nil {
			t.Fatalf("change delivery to pending: %v", err)
		}

		startedAt := time.Now().Add(-100 * time.Millisecond).UTC()
		completedAt := time.Now().UTC()
		responseStatus := http.StatusNoContent

		err = store.FinalizeDeliveryAttempt(
			ctx,
			domain.FinalizeDeliveryParams{
				DeliveryID:         deliveryID,
				AttemptNumber:      1,
				StartedAt:          startedAt,
				CompletedAt:        completedAt,
				ResponseStatus:     &responseStatus,
				ErrorMessage:       nil,
				ResponseDurationMS: 100,
				Status:             domain.DeliveryStatusSucceeded,
			},
		)

		if !errors.Is(err, domain.ErrDeliveryNotProcessing) {
			t.Fatalf(
				"expected ErrDeliveryNotProcessing, got %v",
				err,
			)
		}

		var attemptCount int

		err = testPool.QueryRow(
			ctx,
			`
			SELECT COUNT(*)
			FROM delivery_attempts
			WHERE delivery_id = $1
			`,
			deliveryID,
		).Scan(&attemptCount)
		if err != nil {
			t.Fatalf("count delivery attempts: %v", err)
		}

		if attemptCount != 0 {
			t.Fatalf(
				"attempt count = %d, want 0; transaction did not roll back",
				attemptCount,
			)
		}

		var (
			status             domain.DeliveryStatus
			storedAttemptCount int
		)

		err = testPool.QueryRow(
			ctx,
			`
			SELECT status, attempts_count
			FROM deliveries
			WHERE id = $1
			`,
			deliveryID,
		).Scan(&status, &storedAttemptCount)
		if err != nil {
			t.Fatalf("query delivery: %v", err)
		}

		if status != domain.DeliveryStatusPending {
			t.Errorf(
				"status = %q, want %q",
				status,
				domain.DeliveryStatusPending,
			)
		}

		if storedAttemptCount != 0 {
			t.Errorf(
				"attempts_count = %d, want 0",
				storedAttemptCount,
			)
		}
	})
}
