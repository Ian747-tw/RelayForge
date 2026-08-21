package domain

import "errors"

var (
	ErrEndpointUnavailable = errors.New(
		"one or more endpoints do not exist or are disabled",
	)

	ErrIdempotencyConflict = errors.New(
		"idempotency key already exists",
	)

	ErrNotFound = errors.New("resource not found")

	ErrNoDueDeliveries = errors.New("no due deliveries")

	ErrDeliveryNotProcessing = errors.New("delivery is not processing")
)
