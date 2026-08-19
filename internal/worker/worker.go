package worker

import (
	"context"
	"errors"
	"log/slog"

	"time"

	"github.com/Ian747-tw/relayforge/internal/domain"
)

type Store interface {
	ClaimDueDelivery(
		context.Context,
	) (domain.ClaimedDelivery, error)
}

type Processor interface {
	Process(
		context.Context,
		domain.ClaimedDelivery,
	) error
}

type Worker struct {
	store        Store
	processor    Processor
	pollInterval time.Duration
	logger       *slog.Logger
}

func New(
	store Store,
	processor Processor,
	pollInterval time.Duration,
	logger *slog.Logger,
) *Worker {
	if pollInterval <= 0 {
		panic("worker poll interval must be positive")
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &Worker{
		store:        store,
		processor:    processor,
		pollInterval: pollInterval,
		logger:       logger,
	}
}

func (w *Worker) runOnce(ctx context.Context) error {
	delivery, err := w.store.ClaimDueDelivery(ctx)
	if err != nil {
		return err
	}

	return w.processor.Process(ctx, delivery)
}

func waitForNextPoll(
	ctx context.Context,
	delay time.Duration,
) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (w *Worker) Run(ctx context.Context) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		err := w.runOnce(ctx)

		switch {
		case err == nil:
			//work succeed
			continue
		case errors.Is(err, domain.ErrNoDueDeliveries):
			//empty queue
		case errors.Is(err, context.Canceled):
			return
		case errors.Is(err, context.DeadlineExceeded):
			if ctx.Err() != nil {
				return
			}

			w.logger.Error(
				"worker operation timed out",
				"error", err,
			)
		default:
			w.logger.Error(
				"worker iteration failed",
				"error", err,
			)
		}

		if !waitForNextPoll(ctx, w.pollInterval) {
			return
		}
	}
}
