package worker

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/Ian747-tw/relayforge/internal/domain"
)

type fakeStore struct {
	claimFunc func(
		context.Context,
	) (domain.ClaimedDelivery, error)
}

func (f *fakeStore) ClaimDueDelivery(
	ctx context.Context,
) (domain.ClaimedDelivery, error) {
	if f.claimFunc == nil {
		panic("unexpected call to ClaimDueDelivery")
	}

	return f.claimFunc(ctx)
}

type fakeProcessor struct {
	processFunc func(
		context.Context,
		domain.ClaimedDelivery,
	) error
}

func (f *fakeProcessor) Process(
	ctx context.Context,
	delivery domain.ClaimedDelivery,
) error {
	if f.processFunc == nil {
		panic("unexpected call to Process")
	}

	return f.processFunc(ctx, delivery)
}

func TestRunOnceClaimsAndProcessesDelivery(t *testing.T) {
	ctx := context.Background()

	want := domain.ClaimedDelivery{
		ID:          42,
		EventID:     10,
		EndpointID:  3,
		EventType:   "user.created",
		EndpointURL: "https://example.com/webhook",
	}

	store := &fakeStore{
		claimFunc: func(
			ctx context.Context,
		) (domain.ClaimedDelivery, error) {
			return want, nil
		},
	}

	var processed bool

	processor := &fakeProcessor{
		processFunc: func(
			ctx context.Context,
			delivery domain.ClaimedDelivery,
		) error {
			processed = true

			if delivery.ID != want.ID {
				t.Errorf("expected delivery ID %d, got %d", want.ID, delivery.ID)
			}

			return nil
		},
	}

	worker := New(
		store,
		processor,
		time.Second,
		slog.Default(),
	)

	if err := worker.runOnce(ctx); err != nil {
		t.Fatalf("runOnce error: %v", err)
	}

	if !processed {
		t.Fatal("processor was not called")
	}
}

func TestRunOnceEmptyQueue(t *testing.T) {
	ctx := context.Background()

	store := &fakeStore{
		claimFunc: func(
			ctx context.Context,
		) (domain.ClaimedDelivery, error) {
			return domain.ClaimedDelivery{}, domain.ErrNoDueDeliveries
		},
	}

	processor := &fakeProcessor{
		processFunc: func(
			ctx context.Context,
			delivery domain.ClaimedDelivery,
		) error {
			t.Fatal("processor should not be called")
			return nil
		},
	}

	worker := New(
		store,
		processor,
		time.Second,
		slog.Default(),
	)

	err := worker.runOnce(ctx)

	if !errors.Is(err, domain.ErrNoDueDeliveries) {
		t.Fatalf("expected ErrNoDueDeliveries, got %v", err)
	}
}

func TestRunStopsWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	store := &fakeStore{
		claimFunc: func(
			context.Context,
		) (domain.ClaimedDelivery, error) {
			return domain.ClaimedDelivery{},
				domain.ErrNoDueDeliveries
		},
	}

	processor := &fakeProcessor{
		processFunc: func(
			context.Context,
			domain.ClaimedDelivery,
		) error {
			t.Fatal("processor should not be called")
			return nil
		},
	}

	worker := New(
		store,
		processor,
		time.Hour,
		slog.Default(),
	)

	done := make(chan struct{})

	go func() {
		worker.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		//correct
	case <-time.After(500 * time.Millisecond):
		t.Fatal("worker did not stop promptly")
	}
}

func TestRunDoesNotBusyLoopWhenQueueEmpty(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var callCount int

	store := &fakeStore{
		claimFunc: func(
			context.Context,
		) (domain.ClaimedDelivery, error) {
			callCount++
			return domain.ClaimedDelivery{},
				domain.ErrNoDueDeliveries
		},
	}

	processor := &fakeProcessor{
		processFunc: func(
			context.Context,
			domain.ClaimedDelivery,
		) error {
			t.Fatal("processor should not be called")
			return nil
		},
	}

	worker := New(
		store,
		processor,
		50*time.Millisecond,
		slog.Default(),
	)

	done := make(chan struct{})

	go func() {
		worker.Run(ctx)
		close(done)
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	got := callCount

	if got < 2 || got > 6 {
		t.Fatalf("claim calls = %d", got)
	}

}

func TestProcessorErrorNotKillWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var storeCallCount int

	store := &fakeStore{
		claimFunc: func(
			context.Context,
		) (domain.ClaimedDelivery, error) {
			if storeCallCount == 0 {
				storeCallCount++
				return domain.ClaimedDelivery{}, nil
			}
			storeCallCount++
			return domain.ClaimedDelivery{}, domain.ErrNoDueDeliveries
		},
	}

	processor := &fakeProcessor{
		processFunc: func(
			context.Context,
			domain.ClaimedDelivery,
		) error {
			return errors.New("processor error")
		},
	}

	worker := New(
		store,
		processor,
		50*time.Millisecond,
		slog.Default(),
	)

	done := make(chan struct{})

	go func() {
		worker.Run(ctx)
		close(done)
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	if storeCallCount < 2 {
		t.Fatalf("storeCallCount: %d, expected >= 2", storeCallCount)
	}

}
