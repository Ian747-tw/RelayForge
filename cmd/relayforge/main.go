package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Ian747-tw/relayforge/internal/domain"
	"github.com/Ian747-tw/relayforge/internal/httpapi"
	"github.com/Ian747-tw/relayforge/internal/store/postgres"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf(
			"usage: relayforge <ping|seed|inspect>",
		)
	}

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		return fmt.Errorf("TEST_DATABASE_URL is required")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("create database pool: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	store := postgres.New(pool)

	switch os.Args[1] {
	case "ping":
		return runPing(ctx, pool)

	case "seed":
		return runSeed(ctx, store)

	case "inspect":
		return runInspect(ctx, store)

	case "serve":
		return runServer(store)

	default:
		return fmt.Errorf(
			"unknown command: %q",
			os.Args[1],
		)
	}
}

func runPing(
	ctx context.Context,
	pool *pgxpool.Pool,
) error {
	var databaseName string

	err := pool.QueryRow(
		ctx,
		"SELECT current_database()",
	).Scan(&databaseName)
	if err != nil {
		return fmt.Errorf("query database name: %w", err)
	}

	fmt.Printf("connected to %s\n", databaseName)
	return nil
}

func runSeed(
	ctx context.Context,
	store *postgres.Store,
) error {
	fmt.Println("Creating test data...")

	// TODO:
	// 1. Call CreateEndpoint twice.
	// 2. Print both endpoint IDs.
	// 3. Call CreateEventWithDeliveries with both IDs.
	// 4. Print the event ID.
	//
	// endpointA, err := store.CreateEndpoint(...)
	// ...
	//
	// event, err := store.CreateEventWithDeliveries(...)
	// ...
	endpointA, err := store.CreateEndpoint(ctx, "example-1", "secret-1")
	if err != nil {
		return fmt.Errorf("create endpoint: %v", err)
	}
	fmt.Printf("endpoint id %d created\n", endpointA.ID)

	endpointB, err := store.CreateEndpoint(ctx, "example-2", "secret-2")
	if err != nil {
		return fmt.Errorf("create endpoint: %v", err)
	}
	fmt.Printf("endpoint id %d created\n", endpointB.ID)

	testData := json.RawMessage(`{"task_id": 42, "status": "success", "payload": [1, 2, 3]}`)
	endpoint_ids := []int64{endpointA.ID, endpointB.ID, 99999}

	event_params := domain.CreateEventParams{
		EventType:      "type-1",
		Payload:        testData,
		IdempotencyKey: "sk-1",
		EndpointIDs:    endpoint_ids,
	}

	new_event, _, err := store.CreateEventWithDeliveries(ctx, event_params)
	if err != nil {
		return fmt.Errorf("create event: %v", err)
	}

	fmt.Printf("event id %d created\n", new_event.ID)

	return nil
}

func runInspect(
	ctx context.Context,
	store *postgres.Store,
) error {
	fmt.Println("Inspecting stored data...")

	// TODO:
	// Call your read queries and print their results.
	//
	// endpoints, err := store.ListActiveEndpoints(ctx)
	// ...
	//
	// delivery, err := store.GetDelivery(ctx, id)
	// ...
	endpoints, err := store.ListActiveEndpoints(ctx)
	if err != nil {
		return fmt.Errorf("listing endpoints: %v", err)
	}
	fmt.Printf("%d endpoints listed\n", len(endpoints))

	fmt.Println(endpoints)

	return nil
}

func runServer(store *postgres.Store) error {
	api := httpapi.New(store)

	server := &http.Server{
		Addr:              httpAddress(),
		Handler:           api,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	fmt.Printf("RelayForge listening on %s\n", server.Addr)

	err := server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server HTTP: %w", err)
	}

	return nil
}

func httpAddress() string {
	address := os.Getenv("HTTP_ADDR")
	if address == "" {
		return ":8080"
	}

	return address
}
