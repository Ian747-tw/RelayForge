package domain

import (
	"encoding/json"
	"time"
)

type Endpoint struct {
	ID         int64
	URL        string
	Secret     string
	CreatedAt  time.Time
	DisabledAt *time.Time
}

type Event struct {
	ID             int64
	EventType      string
	CreatedAt      time.Time
	Payload        json.RawMessage
	IdempotencyKey string
}

type Delivery struct {
	ID             int64
	EventID        int64
	EndpointID     int64
	NextAttemptDue *time.Time
	Status         DeliveryStatus
	AttemptsCount  int
	CreatedAt      time.Time
	CompletedAt    *time.Time
	ClaimedAt      *time.Time
}

type DeliveryAttempt struct {
	ID                 int64
	DeliveryID         int64
	AttemptNumber      int
	ResponseStatus     *int
	ErrorMessage       *string
	StartedAt          time.Time
	CompletedAt        *time.Time
	ResponseDurationMS *int
}

type CreateEventParams struct {
	EventType      string
	Payload        json.RawMessage
	IdempotencyKey string
	EndpointIDs    []int64
}

type DeliveryDetails struct {
	ID             int64
	EventID        int64
	EndpointID     int64
	Status         DeliveryStatus
	AttemptsCount  int
	NextAttemptDue *time.Time
	CreatedAt      time.Time
	ClaimedAt      *time.Time

	EventType   string
	Payload     json.RawMessage
	EndpointURL string
}

type DeliveryStatus string

const (
	DeliveryStatusPending        DeliveryStatus = "pending"
	DeliveryStatusProcessing     DeliveryStatus = "processing"
	DeliveryStatusRetryScheduled DeliveryStatus = "retry_scheduled"
	DeliveryStatusSucceeded      DeliveryStatus = "succeeded"
	DeliveryStatusDead           DeliveryStatus = "dead"
)

type ClaimedDelivery struct {
	ID           int64
	EventID      int64
	EndpointID   int64
	EventType    string
	Payload      json.RawMessage
	EndpointURL  string
	AttemptCount int
	ClaimedAt    time.Time
}
