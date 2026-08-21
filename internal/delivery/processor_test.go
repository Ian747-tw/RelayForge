package delivery

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Ian747-tw/relayforge/internal/domain"
)

type fakeSender struct {
	sendFunc func(
		context.Context,
		domain.ClaimedDelivery,
	) (SendResult, error)
}

func (f *fakeSender) Send(
	ctx context.Context,
	claimed domain.ClaimedDelivery,
) (SendResult, error) {
	return f.sendFunc(ctx, claimed)
}

type fakeFinalizer struct {
	called bool
	params domain.FinalizeDeliveryParams
	err    error
}

func (f *fakeFinalizer) FinalizeDeliveryAttempt(
	ctx context.Context,
	params domain.FinalizeDeliveryParams,
) error {
	f.called = true
	f.params = params
	return f.err
}

func intPointer(value int) *int {
	return &value
}

func stringPointer(value string) *string {
	return &value
}

func equalOptionalInt(a, b *int) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	return *a == *b
}

func equalOptionalString(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	return *a == *b
}

func formatOptionalInt(value *int) any {
	if value == nil {
		return nil
	}

	return *value
}

func formatOptionalString(value *string) any {
	if value == nil {
		return nil
	}

	return *value
}

func TestProcessorProcess(t *testing.T) {
	baseTime := time.Date(
		2026,
		time.August,
		21,
		12,
		0,
		0,
		0,
		time.UTC,
	)

	transportErr := errors.New("connection refused")

	tests := []struct {
		name string

		statusCode   int
		senderErr    error
		attemptCount int

		wantStatus         domain.DeliveryStatus
		wantResponseStatus *int
		wantErrorMessage   *string
		wantNextAttempt    bool
	}{
		{
			name:               "204 succeeds",
			statusCode:         http.StatusNoContent,
			attemptCount:       0,
			wantStatus:         domain.DeliveryStatusSucceeded,
			wantResponseStatus: intPointer(http.StatusNoContent),
		},
		{
			name:               "500 schedules retry",
			statusCode:         http.StatusInternalServerError,
			attemptCount:       0,
			wantStatus:         domain.DeliveryStatusRetryScheduled,
			wantResponseStatus: intPointer(http.StatusInternalServerError),
			wantNextAttempt:    true,
		},
		{
			name:               "400 becomes dead",
			statusCode:         http.StatusBadRequest,
			attemptCount:       0,
			wantStatus:         domain.DeliveryStatusDead,
			wantResponseStatus: intPointer(http.StatusBadRequest),
		},
		{
			name:             "transport error schedules retry",
			statusCode:       0,
			senderErr:        transportErr,
			attemptCount:     0,
			wantStatus:       domain.DeliveryStatusRetryScheduled,
			wantErrorMessage: stringPointer(transportErr.Error()),
			wantNextAttempt:  true,
		},
		{
			name:               "retryable response on final attempt becomes dead",
			statusCode:         http.StatusInternalServerError,
			attemptCount:       7,
			wantStatus:         domain.DeliveryStatusDead,
			wantResponseStatus: intPointer(http.StatusInternalServerError),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SendResult{
				StatusCode:  tt.statusCode,
				StartedAt:   baseTime,
				CompletedAt: baseTime.Add(125 * time.Millisecond),
				Duration:    125 * time.Millisecond,
			}

			sender := &fakeSender{
				sendFunc: func(
					ctx context.Context,
					claimed domain.ClaimedDelivery,
				) (SendResult, error) {
					return result, tt.senderErr
				},
			}

			finalizer := &fakeFinalizer{}

			processor := NewProcessor(
				sender,
				finalizer,
				8,
				30*time.Second,
			)

			claimed := domain.ClaimedDelivery{
				ID:           10,
				EventID:      20,
				EndpointID:   30,
				EventType:    "test.event",
				AttemptCount: tt.attemptCount,
			}

			err := processor.Process(context.Background(), claimed)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			if !finalizer.called {
				t.Fatal("finalizer was not called")
			}

			got := finalizer.params

			if got.DeliveryID != claimed.ID {
				t.Errorf(
					"DeliveryID = %d, want %d",
					got.DeliveryID,
					claimed.ID,
				)
			}

			wantAttemptNumber := tt.attemptCount + 1
			if got.AttemptNumber != wantAttemptNumber {
				t.Errorf(
					"AttemptNumber = %d, want %d",
					got.AttemptNumber,
					wantAttemptNumber,
				)
			}

			if got.Status != tt.wantStatus {
				t.Errorf(
					"Status = %q, want %q",
					got.Status,
					tt.wantStatus,
				)
			}

			if !equalOptionalInt(
				got.ResponseStatus,
				tt.wantResponseStatus,
			) {
				t.Errorf(
					"ResponseStatus = %v, want %v",
					formatOptionalInt(got.ResponseStatus),
					formatOptionalInt(tt.wantResponseStatus),
				)
			}

			if !equalOptionalString(
				got.ErrorMessage,
				tt.wantErrorMessage,
			) {
				t.Errorf(
					"ErrorMessage = %v, want %v",
					formatOptionalString(got.ErrorMessage),
					formatOptionalString(tt.wantErrorMessage),
				)
			}

			if !got.StartedAt.Equal(result.StartedAt) {
				t.Errorf(
					"StartedAt = %v, want %v",
					got.StartedAt,
					result.StartedAt,
				)
			}

			if !got.CompletedAt.Equal(result.CompletedAt) {
				t.Errorf(
					"CompletedAt = %v, want %v",
					got.CompletedAt,
					result.CompletedAt,
				)
			}

			if got.ResponseDurationMS != 125 {
				t.Errorf(
					"ResponseDurationMS = %d, want 125",
					got.ResponseDurationMS,
				)
			}

			if tt.wantNextAttempt {
				if got.NextAttemptDUE == nil {
					t.Fatal(
						"NextAttemptDUE = nil, want non-nil",
					)
				}

				wantNextAttempt := result.CompletedAt.Add(
					30 * time.Second,
				)

				if !got.NextAttemptDUE.Equal(wantNextAttempt) {
					t.Errorf(
						"NextAttemptDUE = %v, want %v",
						got.NextAttemptDUE,
						wantNextAttempt,
					)
				}
			} else if got.NextAttemptDUE != nil {
				t.Errorf(
					"NextAttemptDUE = %v, want nil",
					got.NextAttemptDUE,
				)
			}
		})
	}
}

func TestProcessorProcessCancellationDoesNotFinalize(t *testing.T) {
	sender := &fakeSender{
		sendFunc: func(
			ctx context.Context,
			claimed domain.ClaimedDelivery,
		) (SendResult, error) {
			return SendResult{
				StatusCode:  0,
				StartedAt:   time.Now(),
				CompletedAt: time.Now(),
			}, context.Canceled
		},
	}

	finalizer := &fakeFinalizer{}

	processor := NewProcessor(
		sender,
		finalizer,
		8,
		30*time.Second,
	)

	err := processor.Process(
		context.Background(),
		domain.ClaimedDelivery{
			ID:           10,
			AttemptCount: 0,
		},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"Process() error = %v, want context.Canceled",
			err,
		)
	}

	if finalizer.called {
		t.Fatal(
			"finalizer was called after processing was interrupted",
		)
	}
}

func TestProcessorProcessReturnsFinalizerError(t *testing.T) {
	finalizeErr := errors.New("database unavailable")

	baseTime := time.Now().UTC()

	sender := &fakeSender{
		sendFunc: func(
			ctx context.Context,
			claimed domain.ClaimedDelivery,
		) (SendResult, error) {
			return SendResult{
				StatusCode:  http.StatusNoContent,
				StartedAt:   baseTime,
				CompletedAt: baseTime.Add(50 * time.Millisecond),
				Duration:    50 * time.Millisecond,
			}, nil
		},
	}

	finalizer := &fakeFinalizer{
		err: finalizeErr,
	}

	processor := NewProcessor(
		sender,
		finalizer,
		8,
		30*time.Second,
	)

	err := processor.Process(
		context.Background(),
		domain.ClaimedDelivery{
			ID:           10,
			AttemptCount: 0,
		},
	)

	if !errors.Is(err, finalizeErr) {
		t.Fatalf(
			"Process() error = %v, want wrapped %v",
			err,
			finalizeErr,
		)
	}

	if !finalizer.called {
		t.Fatal("finalizer was not called")
	}
}
