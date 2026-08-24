package worker

import (
	"context"
	"fmt"
	"log"
	"time"
)

type RecoveryRunner struct {
	store    RecoveryStore
	lease    time.Duration
	interval time.Duration
}

func NewRecoveryRunner(
	store RecoveryStore,
	lease time.Duration,
	interval time.Duration,
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

	return &RecoveryRunner{
		store:    store,
		lease:    lease,
		interval: interval,
	}, nil
}

func (r *RecoveryRunner) Run(ctx context.Context) error {
	if err := r.recover(ctx); err != nil {
		log.Printf("initial stale-delivery recovery: %v", err)
	}

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			if err := r.recover(ctx); err != nil {
				log.Printf("recover stale deliveries: %v", err)
			}
		}
	}
}

func (r *RecoveryRunner) recover(ctx context.Context) error {
	now := time.Now().UTC()

	recovered, err := r.store.RecoverStaleDeliveries(
		ctx,
		now.Add(-r.lease),
		now,
	)
	if err != nil {
		return err
	}

	if recovered > 0 {
		log.Printf(
			"recoverd %d stale deliveries",
			recovered,
		)
	}

	return nil
}
