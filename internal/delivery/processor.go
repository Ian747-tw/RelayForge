package delivery

import (
	"context"
	"fmt"

	"github.com/Ian747-tw/relayforge/internal/domain"
)

type WebhookSender interface {
	Send(
		context.Context,
		domain.ClaimedDelivery,
	) (SendResult, error)
}

type Finalizer interface {
	FinalizeDeliveryAttempt(
		context.Context,
		domain.FinalizeDeliveryParams,
	) error
}

type Processor struct {
	sender      WebhookSender
	finalizer   Finalizer
	maxAttempts int
	retryPolicy RetryPolicy
}

func NewProcessor(
	sender WebhookSender,
	finalizer Finalizer,
	maxAttempts int,
	retryPolicy RetryPolicy,
) *Processor {
	if maxAttempts <= 0 {
		panic("max attempts must be positive")
	}

	return &Processor{
		sender:      sender,
		finalizer:   finalizer,
		maxAttempts: maxAttempts,
		retryPolicy: retryPolicy,
	}
}

func (p *Processor) Process(
	ctx context.Context,
	claimed domain.ClaimedDelivery,
) error {
	result, sendErr := p.sender.Send(ctx, claimed)
	outcome := ClassifyResult(result.StatusCode, sendErr)

	if outcome == OutcomeInterrupted {
		return sendErr
	}

	attemptNumber := claimed.AttemptCount + 1

	params := domain.FinalizeDeliveryParams{
		DeliveryID:         claimed.ID,
		AttemptNumber:      attemptNumber,
		StartedAt:          result.StartedAt,
		CompletedAt:        result.CompletedAt,
		ResponseDurationMS: result.Duration.Milliseconds(),
	}

	if result.StatusCode != 0 {
		status := result.StatusCode
		params.ResponseStatus = &status
	}

	if sendErr != nil {
		message := sendErr.Error()
		if len(message) > 1000 {
			message = message[:1000]
		}
		params.ErrorMessage = &message
	}

	switch outcome {
	case OutcomeSuccess:
		params.Status = domain.DeliveryStatusSucceeded

	case OutcomePermanentFailure:
		params.Status = domain.DeliveryStatusDead

	case OutcomeRetry:
		if attemptNumber >= p.maxAttempts {
			params.Status = domain.DeliveryStatusDead
		} else {
			delay := p.retryPolicy.NextDelay(
				attemptNumber,
				result.StatusCode,
				result.RetryAfter,
				result.CompletedAt,
			)

			nextAttemptDue := result.CompletedAt.Add(delay)

			params.Status = domain.DeliveryStatusRetryScheduled
			params.NextAttemptDUE = &nextAttemptDue
		}

	}

	if err := p.finalizer.FinalizeDeliveryAttempt(ctx, params); err != nil {
		return fmt.Errorf("finalize delivery attempt: %w", err)
	}

	return nil

}
