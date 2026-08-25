package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type RecoveryRunner struct {
	store    RecoveryStore
	lease    time.Duration
	interval time.Duration
	logger   *slog.Logger
}

func NewRecoveryRunner(
	store RecoveryStore,
	lease time.Duration,
	interval time.Duration,
	logger *slog.Logger,
) (*RecoveryRunner, error) {
	if store == nil {
		return nil, fmt.Errorf("recovery store is nil")
	}

	if lease <= 0 {
		return nil, fmt.Errorf("claim lease must be positive")
	}

	if interval <= 0 {
		return nil, fmt.Errorf("recovery interval must be positive")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger is nil")
	}

	return &RecoveryRunner{
		store:    store,
		lease:    lease,
		interval: interval,
		logger:   logger,
	}, nil
}

func (r *RecoveryRunner) Run(ctx context.Context) {
	if err := r.recover(ctx); err != nil {
		if ctx.Err() != nil {
			return
		}

		r.logger.Error(
			"initial stale-delivery recovery failed",
			"error", err,
		)
	}

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			if err := r.recover(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}

				r.logger.Error(
					"recover stale deliveries",
					"error", err,
				)
			}
		}
	}
}

func (r *RecoveryRunner) recover(
	ctx context.Context,
) error {
	now := time.Now().UTC()

	recovered, err := r.store.RecoverStaleDeliveries(
		ctx,
		now.Add(-r.lease),
		now,
	)
	if err != nil {
		return fmt.Errorf(
			"recover stale deliveries: %w",
			err,
		)
	}

	if recovered > 0 {
		r.logger.Info(
			"recovered stale deliveries",
			"count", recovered,
		)
	}

	return nil
}
