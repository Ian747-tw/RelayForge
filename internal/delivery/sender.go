package sender

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/Ian747-tw/relayforge/internal/domain"
)

type SendResult struct {
	StatusCode  int
	StartedAt   time.Time
	CompletedAt time.Time
	Duration    time.Duration
}

type Sender struct {
	client *http.Client
}

func NewSender(timeout time.Duration) *Sender {
	if timeout <= 0 {
		panic("delivery timeout must be positive")
	}

	return &Sender{
		client: &http.Client{
			Timeout: timeout,

			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (s *Sender) Send(
	ctx context.Context,
	delivery domain.ClaimedDelivery,
) (SendResult, error) {
	startedAt := time.Now()

	result := SendResult{
		StartedAt: startedAt,
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		delivery.EndpointURL,
		bytes.NewReader(delivery.Payload),
	)
	if err != nil {
		result.CompletedAt = time.Now()
		result.Duration = result.CompletedAt.Sub(startedAt)
		return result, fmt.Errorf("created webhook request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(
		"X-RelayForge-Event-ID",
		strconv.FormatInt(delivery.EventID, 10),
	)
	req.Header.Set(
		"X-RelayForge-Delivery-ID",
		strconv.FormatInt(delivery.ID, 10),
	)
	req.Header.Set(
		"X-RelayForge-Event-Type",
		delivery.EventType,
	)

	resp, err := s.client.Do(req)

	result.CompletedAt = time.Now()
	result.Duration = result.CompletedAt.Sub(startedAt)

	if err != nil {
		return result, fmt.Errorf(
			"send webhook request: %w",
			err,
		)
	}
	defer resp.Body.Close()

	result.StatusCode = resp.StatusCode

	_, copyErr := io.Copy(
		io.Discard,
		io.LimitReader(resp.Body, 64<<10),
	)
	if copyErr != nil {
		return result, fmt.Errorf(
			"discard webhook resposne body: %w",
			copyErr,
		)
	}

	return result, nil
}
