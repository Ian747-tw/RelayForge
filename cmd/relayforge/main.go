package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Ian747-tw/relayforge/internal/delivery"
	"github.com/Ian747-tw/relayforge/internal/domain"
	"github.com/Ian747-tw/relayforge/internal/httpapi"
	"github.com/Ian747-tw/relayforge/internal/store/postgres"
	"github.com/Ian747-tw/relayforge/internal/worker"
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

	databaseURL := "postgresql://relayforge:relayforge_dev_password@localhost:5432/relayforge_test"

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
		logger := slog.New(
			slog.NewTextHandler(
				os.Stdout,
				&slog.HandlerOptions{
					Level: slog.LevelInfo,
				},
			),
		)
		return runServe(store, logger)

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

	endpoints, err := store.ListActiveEndpoints(ctx)
	if err != nil {
		return fmt.Errorf("listing endpoints: %v", err)
	}
	fmt.Printf("%d endpoints listed\n", len(endpoints))

	fmt.Println(endpoints)

	return nil
}

func runServe(store *postgres.Store, logger *slog.Logger) error {
	//config
	const (
		workerCount       = 4
		workerPollDelay   = 250 * time.Millisecond
		senderTimeout     = 10 * time.Second
		claimLease        = 60 * time.Second
		recoveryInterval  = 15 * time.Second
		httpStopTimeout   = 10 * time.Second
		workerStopTimeout = 10 * time.Second
		maxAttempts       = 8
	)

	if store == nil {
		return fmt.Errorf("store is nil")
	}

	if logger == nil {
		return fmt.Errorf("logger is nil")
	}

	if claimLease <= senderTimeout {
		return fmt.Errorf("claim lease must exceed sender timeout")
	}

	//construct dependencies
	sender := delivery.NewSender(senderTimeout)

	retryPolicy, err := delivery.NewRetryPolicy(
		5*time.Second,
		15*time.Minute,
		delivery.RandomJitter,
	)
	if err != nil {
		return fmt.Errorf("create retry policy: %w", err)
	}

	processor := delivery.NewProcessor(
		sender,
		store,
		8,
		retryPolicy,
	)

	recoveryLogger := logger.With(
		"component", "stale_claim_recovery",
	)

	recoveryRunner, err := worker.NewRecoveryRunner(
		store,
		claimLease,
		recoveryInterval,
		recoveryLogger,
	)
	if err != nil {
		return fmt.Errorf("create recovery runner: %w", err)
	}

	//create lifecycle contexts
	signalCtx, stopSignals := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignals()

	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()

	//start delivery workers
	var backgroundWG sync.WaitGroup

	for workerID := range workerCount {
		workerLogger := logger.With(
			"component", "delivery_worker",
			"worker_id", workerID,
		)

		deliveryWorker := worker.New(
			store,
			processor,
			workerPollDelay,
			workerLogger,
		)

		backgroundWG.Add(1)

		go func(w *worker.Worker) {
			defer backgroundWG.Done()
			w.Run(workerCtx)
		}(deliveryWorker)
	}

	//start one stale-claim recovery runner
	backgroundWG.Add(1)

	go func() {
		defer backgroundWG.Done()
		recoveryRunner.Run(workerCtx)
	}()

	//construct existing HTTP API
	api := httpapi.New(store)

	httpServer := &http.Server{
		Addr:              ":8080",
		Handler:           api,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	//listen and serve
	serverResult := make(chan error, 1)

	go func() {
		serverResult <- httpServer.ListenAndServe()
	}()

	logger.Info(
		"RelayForge started",
		"address", httpServer.Addr,
		"worker_count", workerCount,
		"sender_timeout", senderTimeout,
		"claim_lease", claimLease,
		"recovery_interval", recoveryInterval,
	)

	var runErr error
	serverAlreadyStopped := false

	select {
	case <-signalCtx.Done():
		logger.Info(
			"shutdown signal received",
			"signal_error", signalCtx.Err(),
		)
	case err := <-serverResult:
		serverAlreadyStopped = true

		if err == nil {
			runErr = errors.New(
				"HTTP server stopped unexpectedly without error",
			)
		} else if errors.Is(err, http.ErrServerClosed) {
			runErr = errors.New(
				"HTTP server was closed unexpectedly",
			)
		} else {
			runErr = fmt.Errorf(
				"HTTP server failed: %w",
				err,
			)
		}
	}

	//first stop accepting inbound API requests and allow
	//active handlers to finish.
	httpShutdownCtx, cancelHTTPShutdown :=
		context.WithTimeout(
			context.Background(),
			httpStopTimeout,
		)

	httpShutdownErr :=
		httpServer.Shutdown(httpShutdownCtx)

	cancelHTTPShutdown()

	if httpShutdownErr != nil {
		runErr = errors.Join(
			runErr,
			fmt.Errorf(
				"graceful HTTP shutdown: %w",
				httpShutdownErr,
			),
		)

		//shutdown did not finish gracefully. Force remaining
		//ordinary HTTP connections close
		if closeErr := httpServer.Close(); closeErr != nil {
			runErr = errors.Join(
				runErr,
				fmt.Errorf(
					"force close HTTP server: %w",
					closeErr,
				),
			)
		}
	}

	//if ListenAndServe had not already returned, wait for it
	if !serverAlreadyStopped {
		select {
		case err := <-serverResult:
			if err != nil &&
				!errors.Is(err, http.ErrServerClosed) {
				runErr = errors.Join(
					runErr,
					fmt.Errorf(
						"HTTP server exit: %w",
						err,
					),
				)
			}

		case <-time.After(time.Second):
			runErr = errors.Join(
				runErr,
				errors.New(
					"HTTP server goroutine did not exit",
				),
			)
		}
	}

	//stop background workers
	cancelWorkers()

	backgroundDone := make(chan struct{})

	go func() {
		backgroundWG.Wait()
		close(backgroundDone)
	}()

	workerShutdownCtx, cancelWorkerShutdown :=
		context.WithTimeout(
			context.Background(),
			workerStopTimeout,
		)
	defer cancelWorkerShutdown()

	select {
	case <-backgroundDone:
		logger.Info(
			"background workers stopped",
		)

	case <-workerShutdownCtx.Done():
		runErr = errors.Join(
			runErr,
			fmt.Errorf(
				"background shutdown: %w",
				workerShutdownCtx.Err(),
			),
		)
	}

	if runErr != nil {
		logger.Error(
			"RelayForge stopped with error",
			"error", runErr,
		)

		return runErr
	}

	logger.Info("RelayForge stopped cleanly")

	return nil

}

func httpAddress() string {
	address := os.Getenv("HTTP_ADDR")
	if address == "" {
		return ":8080"
	}

	return address
}
