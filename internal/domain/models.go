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
	Status         string
	AttemptsCount  int
	CreatedAt      time.Time
	CompletedAt    *time.Time
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
	Status         string
	AttemptsCount  int
	NextAttemptDue *time.Time
	CreatedAt      time.Time

	EventType   string
	Payload     json.RawMessage
	EndpointURL string
}
