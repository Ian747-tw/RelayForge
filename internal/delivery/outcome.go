package delivery

import (
	"context"
	"errors"
	"net/http"
)

type Outcome int

const (
	OutcomeSuccess Outcome = iota
	OutcomeRetry
	OutcomePermanentFailure
	OutcomeInterrupted
)

func ClassifyResult(status int, senderErr error) Outcome {
	if senderErr != nil {
		if errors.Is(senderErr, context.Canceled) {
			return OutcomeInterrupted
		}

		// No HTTP response was received. For the current MVP,
		// transport errors and timeouts are retryable.
		return OutcomeRetry
	}

	switch {
	case status >= 200 && status <= 299:
		return OutcomeSuccess

	case status == http.StatusRequestTimeout, // 408
		status == http.StatusTooEarly,        // 425
		status == http.StatusTooManyRequests, // 429
		status >= 500 && status <= 599:
		return OutcomeRetry

	default:
		return OutcomePermanentFailure
	}
}
