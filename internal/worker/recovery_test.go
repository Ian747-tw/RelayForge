package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

type recoveryCall struct {
	StaleBefore time.Time
	RetryAt     time.Time
}

type fakeRecoveryStore struct {
	calls chan recoveryCall
	err   error
}

func (f *fakeRecoveryStore) RecoverStaleDeliveries(
	ctx context.Context,
	staleBefore time.Time,
	retryAt time.Time,
) (int64, error) {
	call := recoveryCall{
		StaleBefore: staleBefore,
		RetryAt:     retryAt,
	}

	select {
	case f.calls <- call:
		return 1, f.err

	case <-ctx.Done():
		return 0, ctx.Err()
	}

}

func testLogger() *slog.Logger {
	return slog.New(
		slog.NewTextHandler(
			io.Discard,
			nil,
		),
	)
}

func TestRecoveryRunnerRunsImmediatelyAndStops(
	t *testing.T,
) {
	store := &fakeRecoveryStore{
		calls: make(chan recoveryCall, 1),
	}

	runner, err := NewRecoveryRunner(
		store,
		time.Minute,
		time.Hour,
		testLogger(),
	)
	if err != nil {
		t.Fatalf(
			"NewRecoveryRunner() error = %v",
			err,
		)
	}

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	done := make(chan struct{})

	go func() {
		defer close(done)
		runner.Run(ctx)
	}()

	var call recoveryCall

	select {
	case call = <-store.calls:

	case <-time.After(time.Second):
		t.Fatal("initial recovery did not run")
	}

	gotLease := call.RetryAt.Sub(call.StaleBefore)

	if gotLease != time.Minute {
		t.Errorf(
			"recovery lease = %v, want 1m",
			gotLease,
		)
	}

	cancel()

	select {
	case <-done:

	case <-time.After(time.Second):
		t.Fatal(
			"recovery runner did not stop after cancellation",
		)
	}
}
