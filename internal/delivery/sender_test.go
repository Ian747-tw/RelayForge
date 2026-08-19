package sender

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Ian747-tw/relayforge/internal/domain"
)

func TestSenderSendSuccess(t *testing.T) {
	payload := json.RawMessage(`{"user_id":123}`)

	target := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			if r.Method != http.MethodPost {
				t.Errorf("expected method POST, got %s", r.Method)
			}

			if got := r.Header.Get("Content-Type"); got != "application/json" {
				t.Errorf("expected Content Type application/json, got %q", got)
			}

			if got := r.Header.Get("X-RelayForge-Event-ID"); got != "10" {
				t.Errorf("expected Event ID 10, got %q", got)
			}

			if got := r.Header.Get("X-RelayForge-Delivery-ID"); got != "42" {
				t.Errorf("expected delivery ID 42, got %q", got)
			}

			if got := r.Header.Get("X-RelayForge-Event-Type"); got != "user.created" {
				t.Errorf("expected event type user.created, got %q", got)
			}

			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read request body: %v", err)
			}

			if !bytes.Equal(body, payload) {
				t.Errorf("expected body %s, got %s", payload, body)
			}

			w.WriteHeader(http.StatusNoContent)
		}),
	)

	defer target.Close()

	sender := NewSender(time.Second)

	result, err := sender.Send(
		context.Background(),
		domain.ClaimedDelivery{
			ID:          42,
			EventID:     10,
			EventType:   "user.created",
			Payload:     payload,
			EndpointURL: target.URL,
		},
	)
	if err != nil {
		t.Fatalf("Send() error: %v", err)
	}

	if result.StatusCode != http.StatusNoContent {
		t.Errorf("expected %d, got %d", http.StatusNoContent, result.StatusCode)
	}

	if result.StartedAt.IsZero() {
		t.Error("StartedAt is zero")
	}

	if result.CompletedAt.IsZero() {
		t.Error("CompletedAt is zero")
	}

	if result.Duration < 0 {
		t.Errorf("expected duration non-negative, got %v", result.Duration)
	}

}

func TestSenderSendServiceErrorResponse(t *testing.T) {
	target := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			http.Error(
				w,
				"temporary failure",
				http.StatusInternalServerError,
			)
		}),
	)
	defer target.Close()

	sender := NewSender(time.Second)

	result, err := sender.Send(
		context.Background(),
		domain.ClaimedDelivery{
			ID:          1,
			EventID:     1,
			EventType:   "test.event",
			Payload:     json.RawMessage(`{}`),
			EndpointURL: target.URL,
		},
	)
	if err != nil {
		t.Fatalf("Send() returned error for HTTP 500: %v", err)
	}

	if result.StatusCode != http.StatusInternalServerError {
		t.Errorf("unexpected status code %d", result.StatusCode)
	}
}

func TestSenderSendTimeout(t *testing.T) {
	target := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			time.Sleep(200 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}),
	)
	defer target.Close()

	sender := NewSender(20 * time.Millisecond)

	result, err := sender.Send(
		context.Background(),
		domain.ClaimedDelivery{
			ID:          1,
			EventID:     1,
			EventType:   "test.event",
			Payload:     json.RawMessage(`{}`),
			EndpointURL: target.URL,
		},
	)

	if err == nil {
		t.Fatal("Send() error = nil, want timeout")
	}

	if result.StatusCode != 0 {
		t.Errorf("expected status 0, got %d", result.StatusCode)
	}
}

func TestSenderSendCancellation(t *testing.T) {
	reqeustStarted := make(chan struct{})
	releaseHandler := make(chan struct{})

	target := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			close(reqeustStarted)
			select {
			case <-r.Context().Done():
			case <-releaseHandler:
			}
		}),
	)
	defer func() {
		close(releaseHandler)
		target.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sender := NewSender(time.Hour)

	done := make(chan error, 1)

	go func() {
		_, err := sender.Send(
			ctx,
			domain.ClaimedDelivery{
				ID:          1,
				EventID:     1,
				EventType:   "test.event",
				Payload:     json.RawMessage(`{}`),
				EndpointURL: target.URL,
			},
		)

		done <- err
	}()

	select {
	case <-reqeustStarted:
	case <-time.After(time.Second):
		t.Fatal("request never reached test server")
	}
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation error, got nil")
		}

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Send() did not stop after cancellation")
	}

}

func TestSenderConnectionFailure(t *testing.T) {
	target := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
		}),
	)

	targetURL := target.URL
	target.Close()

	sender := NewSender(time.Second)

	result, err := sender.Send(
		context.Background(),
		domain.ClaimedDelivery{
			ID:          1,
			EventID:     1,
			EventType:   "test.event",
			Payload:     json.RawMessage(`{}`),
			EndpointURL: targetURL,
		},
	)

	if err == nil {
		t.Fatal("expected connection failure")
	}

	if result.StatusCode != 0 {
		t.Errorf("expected status 0, got %d", result.StatusCode)
	}
}

func TestSenderDoesNotFollowRedirects(t *testing.T) {
	var redirectedTargetCalled bool

	redirectedTarget := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			redirectedTargetCalled = true
			w.WriteHeader(http.StatusOK)
		}),
	)
	defer redirectedTarget.Close()

	redirectingTarget := httptest.NewServer(
		http.HandlerFunc(func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			http.Redirect(
				w,
				r,
				redirectedTarget.URL,
				http.StatusFound,
			)
		}),
	)
	defer redirectingTarget.Close()

	sender := NewSender(time.Second)

	result, err := sender.Send(
		context.Background(),
		domain.ClaimedDelivery{
			ID:          1,
			EventID:     1,
			EventType:   "test.event",
			Payload:     json.RawMessage(`{}`),
			EndpointURL: redirectingTarget.URL,
		},
	)
	if err != nil {
		t.Fatalf("Send() error: %v", err)
	}

	if result.StatusCode != http.StatusFound {
		t.Errorf("expected status 302, got %d", result.StatusCode)
	}

	if redirectedTargetCalled {
		t.Fatal("sender followed redirect")
	}
}
