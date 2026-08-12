package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Ian747-tw/relayforge/internal/domain"
)

func TestCreateEvent(t *testing.T) {
	var receivedEndpointlen int
	createdAt := time.Date(
		2026,
		time.August,
		7,
		12,
		0,
		0,
		0,
		time.UTC,
	)

	store := &fakeStore{
		createEventWithDeliveriesFunc: func(
			ctx context.Context,
			params domain.CreateEventParams,
		) (domain.Event, bool, error) {
			receivedEndpointlen = len(params.EndpointIDs)
			return domain.Event{
				ID:             5,
				EventType:      params.EventType,
				CreatedAt:      createdAt,
				Payload:        params.Payload,
				IdempotencyKey: params.IdempotencyKey,
			}, true, nil
		},
	}

	server := New(store)

	body := strings.NewReader(`{
		"event_type": "test valid creation",
		"payload": {
			"operation": "buy"
		},
		"endpoint_ids": [1, 2, 3]
	}`)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/events",
		body,
	)

	request.Header.Set(
		"Content-Type",
		"application/json",
	)

	request.Header.Set(
		"Idempotency-Key",
		"test.Create.Event",
	)

	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("expected status created code, got %d", response.Code)
	}

	if receivedEndpointlen != 3 {
		t.Fatalf("expected 3 endpoint len, got %d", receivedEndpointlen)
	}

	var result eventResponse

	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if result.ID != 5 {
		t.Fatalf("expected ID 5, got %d", result.ID)
	}

	if result.IdempotencyKey != "test.Create.Event" {
		t.Fatalf("expected key test.Create.Event, got %s", result.IdempotencyKey)
	}

}

func TestMissingIdempotencyKey(t *testing.T) {
	store := &fakeStore{
		createEventWithDeliveriesFunc: func(
			ctx context.Context,
			params domain.CreateEventParams,
		) (domain.Event, bool, error) {
			return domain.Event{}, false, errors.New("unknown err")
		},
	}

	server := New(store)

	body := strings.NewReader(`{
		"event_type": "test missing key",
		"payload": {
			"operation": "buy"
		},
		"endpoint_ids": [1, 2, 3]
	}`)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/events",
		body,
	)

	request.Header.Set(
		"Content-Type",
		"application/json",
	)

	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", response.Code)
	}

}

func TestInvalidEndpoint(t *testing.T) {
	store := &fakeStore{
		createEventWithDeliveriesFunc: func(
			ctx context.Context,
			params domain.CreateEventParams,
		) (domain.Event, bool, error) {
			return domain.Event{}, false, errors.New("unknown err")
		},
	}

	server := New(store)

	body := strings.NewReader(`{
		"event_type": "test invalid endpoint_ids",
		"payload": {
			"operation": "buy"
		},
		"endpoint_ids": [1, 1, 3]
	}`)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/events",
		body,
	)

	request.Header.Set(
		"Content-Type",
		"application/json",
	)

	request.Header.Set(
		"Idempotency-Key",
		"test.invalid.endpoint",
	)

	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", response.Code)
	}

}

func TestMissingEndpoint(t *testing.T) {
	store := &fakeStore{
		createEventWithDeliveriesFunc: func(
			ctx context.Context,
			params domain.CreateEventParams,
		) (domain.Event, bool, error) {
			return domain.Event{}, false, domain.ErrEndpointUnavailable
		},
	}

	server := New(store)

	body := strings.NewReader(`{
		"event_type": "test missing endpoint_ids",
		"payload": {
			"operation": "buy"
		},
		"endpoint_ids": [1, 2, 3]
	}`)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/events",
		body,
	)

	request.Header.Set(
		"Content-Type",
		"application/json",
	)

	request.Header.Set(
		"Idempotency-Key",
		"test.missing.endpoint",
	)

	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", response.Code)
	}

}

func TestConflictDuplicateKey(t *testing.T) {
	store := &fakeStore{
		createEventWithDeliveriesFunc: func(
			ctx context.Context,
			params domain.CreateEventParams,
		) (domain.Event, bool, error) {
			return domain.Event{}, false, domain.ErrIdempotencyConflict
		},
	}

	server := New(store)

	body := strings.NewReader(`{
		"event_type": "test duplicate key",
		"payload": {
			"operation": "buy"
		},
		"endpoint_ids": [1, 2, 3]
	}`)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/events",
		body,
	)

	request.Header.Set(
		"Content-Type",
		"application/json",
	)

	request.Header.Set(
		"Idempotency-Key",
		"test.duplicate.key",
	)

	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", response.Code)
	}

}

func TestReplayDuplicateKey(t *testing.T) {
	store := &fakeStore{
		createEventWithDeliveriesFunc: func(
			ctx context.Context,
			params domain.CreateEventParams,
		) (domain.Event, bool, error) {
			return domain.Event{
				ID:             5,
				EventType:      params.EventType,
				CreatedAt:      time.Now(),
				Payload:        params.Payload,
				IdempotencyKey: params.IdempotencyKey,
			}, false, nil
		},
	}

	server := New(store)

	body := strings.NewReader(`{
		"event_type": "test duplicate key",
		"payload": {
			"operation": "buy"
		},
		"endpoint_ids": [1, 2, 3]
	}`)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/events",
		body,
	)

	request.Header.Set(
		"Content-Type",
		"application/json",
	)

	request.Header.Set(
		"Idempotency-Key",
		"test.duplicate.key",
	)

	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.Code)
	}

}

func TestStoreError(t *testing.T) {
	store := &fakeStore{
		createEventWithDeliveriesFunc: func(
			ctx context.Context,
			params domain.CreateEventParams,
		) (domain.Event, bool, error) {
			return domain.Event{}, false, errors.New("database exploded")
		},
	}

	server := New(store)

	body := strings.NewReader(`{
		"event_type": "test internal error",
		"payload": {
			"operation": "buy"
		},
		"endpoint_ids": [1, 2, 3]
	}`)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/events",
		body,
	)

	request.Header.Set(
		"Content-Type",
		"application/json",
	)

	request.Header.Set(
		"Idempotency-Key",
		"test.internal.error",
	)

	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", response.Code)
	}

}

func TestHandleGetEvent(t *testing.T) {
	storeFailure := errors.New("database unavailable")

	tests := []struct {
		name       string
		path       string
		getEvent   func(context.Context, int64) (domain.Event, error)
		wantStatus int
		wantCode   string
	}{
		{
			name: "success",
			path: "/v1/events/42",
			getEvent: func(
				ctx context.Context,
				id int64,
			) (domain.Event, error) {
				if id != 42 {
					t.Fatalf("id = %d, want 42", id)
				}

				return domain.Event{
					ID:        42,
					EventType: "user.created",
					Payload:   json.RawMessage(`{"user_id":123}`),
				}, nil
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "invalid non-numeric ID",
			path: "/v1/events/abc",
			getEvent: func(
				context.Context,
				int64,
			) (domain.Event, error) {
				t.Fatal("store should not be called")
				return domain.Event{}, nil
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_id",
		},
		{
			name: "invalid zero ID",
			path: "/v1/events/0",
			getEvent: func(
				context.Context,
				int64,
			) (domain.Event, error) {
				t.Fatal("store should not be called")
				return domain.Event{}, nil
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_id",
		},
		{
			name: "event not found",
			path: "/v1/events/999",
			getEvent: func(
				context.Context,
				int64,
			) (domain.Event, error) {
				return domain.Event{}, domain.ErrNotFound
			},
			wantStatus: http.StatusNotFound,
			wantCode:   "not_found",
		},
		{
			name: "store failure",
			path: "/v1/events/42",
			getEvent: func(
				context.Context,
				int64,
			) (domain.Event, error) {
				return domain.Event{}, storeFailure
			},
			wantStatus: http.StatusInternalServerError,
			wantCode:   "internal_error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{
				getEventFunc: tt.getEvent,
			}

			server := New(store)

			request := httptest.NewRequest(
				http.MethodGet,
				tt.path,
				nil,
			)
			recorder := httptest.NewRecorder()

			server.ServeHTTP(recorder, request)

			if recorder.Code != tt.wantStatus {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					recorder.Code,
					tt.wantStatus,
					recorder.Body.String(),
				)
			}

			if tt.wantCode != "" &&
				!strings.Contains(recorder.Body.String(), tt.wantCode) {
				t.Errorf(
					"body = %s, want error code %q",
					recorder.Body.String(),
					tt.wantCode,
				)
			}
		})
	}
}
