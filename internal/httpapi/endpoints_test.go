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
	"github.com/jackc/pgx/v5"
)

type fakeStore struct {
	createEndpointFunc func(
		context.Context,
		string,
		string,
	) (domain.Endpoint, error)

	listActiveEndpointsFunc func(
		context.Context,
	) ([]domain.Endpoint, error)

	disableEndpointFunc func(
		context.Context,
		int64,
	) error

	createEventWithDeliveriesFunc func(
		context.Context,
		domain.CreateEventParams,
	) (domain.Event, bool, error)

	getEventFunc func(
		context.Context,
		int64,
	) (domain.Event, error)

	getDeliveryFunc func(
		context.Context,
		int64,
	) (domain.DeliveryDetails, error)

	listDeliveryAttemptsFunc func(
		context.Context,
		int64,
	) ([]domain.DeliveryAttempt, error)
}

func (f *fakeStore) CreateEndpoint(
	ctx context.Context,
	url string,
	secret string,
) (domain.Endpoint, error) {
	if f.createEndpointFunc == nil {
		panic("unexpected call to CreateEndpoint")
	}

	return f.createEndpointFunc(ctx, url, secret)
}

func (f *fakeStore) ListActiveEndpoints(
	ctx context.Context,
) ([]domain.Endpoint, error) {
	if f.listActiveEndpointsFunc == nil {
		panic("unexpected call to ListActiveEndpoints")
	}

	return f.listActiveEndpointsFunc(ctx)
}

func (f *fakeStore) DisableEndpoint(
	ctx context.Context,
	endpointID int64,
) error {
	if f.disableEndpointFunc == nil {
		panic("unexpected call to DisableEndpoint")
	}

	return f.disableEndpointFunc(ctx, endpointID)
}

func (f *fakeStore) CreateEventWithDeliveries(
	ctx context.Context,
	params domain.CreateEventParams,
) (domain.Event, bool, error) {
	if f.createEventWithDeliveriesFunc == nil {
		panic(
			"unexpected call to CreateEventWithDeliveries",
		)
	}

	return f.createEventWithDeliveriesFunc(
		ctx,
		params,
	)
}

func (f *fakeStore) GetEvent(
	ctx context.Context,
	id int64,
) (domain.Event, error) {
	if f.getEventFunc == nil {
		panic("unexpected call to GetEvent")
	}

	return f.getEventFunc(ctx, id)
}

func (f *fakeStore) GetDelivery(
	ctx context.Context,
	id int64,
) (domain.DeliveryDetails, error) {
	if f.getDeliveryFunc == nil {
		panic("unexpected call to GetDelivery")
	}

	return f.getDeliveryFunc(ctx, id)
}

func (f *fakeStore) ListDeliveryAttempts(
	ctx context.Context,
	deliveryID int64,
) ([]domain.DeliveryAttempt, error) {
	if f.listDeliveryAttemptsFunc == nil {
		panic("unexpected call to ListDeliveryAttempts")
	}

	return f.listDeliveryAttemptsFunc(ctx, deliveryID)
}

func TestCreateEndpoint(t *testing.T) {
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

	var receivedURL string
	var receivedSecret string

	store := &fakeStore{
		createEndpointFunc: func(
			ctx context.Context,
			url string,
			secret string,
		) (domain.Endpoint, error) {
			receivedURL = url
			receivedSecret = secret

			return domain.Endpoint{
				ID:         42,
				URL:        url,
				Secret:     secret,
				CreatedAt:  createdAt,
				DisabledAt: nil,
			}, nil
		},
	}

	server := New(store)

	body := strings.NewReader(`{
		"url": "https://receiver.example/webhook",
		"secret": "test-secret"
	}`)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/endpoints",
		body,
	)
	request.Header.Set(
		"Content-Type",
		"application/json",
	)

	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf(
			"expected status %d, got %d; body=%s",
			http.StatusCreated,
			response.Code,
			response.Body.String(),
		)
	}

	if receivedURL != "https://receiver.example/webhook" {
		t.Errorf(
			"store received URL %q",
			receivedURL,
		)
	}

	if receivedSecret != "test-secret" {
		t.Errorf(
			"store received secret %q",
			receivedSecret,
		)
	}

	if location := response.Header().Get("Location"); location != "/v1/endpoints/42" {
		t.Errorf(
			"expected Location %q, got %q",
			"/v1/endpoints/42",
			location,
		)
	}

	if strings.Contains(
		response.Body.String(),
		"test-secret",
	) {
		t.Error("response exposed endpoint secret")
	}

	var result endpointResponse

	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if result.ID != 42 {
		t.Errorf(
			"expected ID 42, got %d",
			result.ID,
		)
	}

	if result.URL != "https://receiver.example/webhook" {
		t.Errorf(
			"expected URL %q, got %q",
			"https://receiver.example/webhook",
			result.URL,
		)
	}
}

func TestCreateEndpointRejectsMalformedJSON(
	t *testing.T,
) {
	store := &fakeStore{}

	server := New(store)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/endpoints",
		strings.NewReader(`{"url":`),
	)
	request.Header.Set(
		"Content-Type",
		"application/json",
	)

	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusBadRequest,
			response.Code,
		)
	}
}

func TestListEndpoints(t *testing.T) {
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
		listActiveEndpointsFunc: func(
			ctx context.Context,
		) ([]domain.Endpoint, error) {
			return []domain.Endpoint{
				{
					ID:        1,
					URL:       "https://a.example/webhook",
					Secret:    "secret-a",
					CreatedAt: createdAt,
				},
				{
					ID:        2,
					URL:       "https://b.example/webhook",
					Secret:    "secret-b",
					CreatedAt: createdAt,
				},
			}, nil
		},
	}

	server := New(store)

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/endpoints",
		nil,
	)

	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status %d, got %d; body=%s",
			http.StatusOK,
			response.Code,
			response.Body.String(),
		)
	}

	var result listEndpointsResponse

	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(result.Endpoints) != 2 {
		t.Fatalf(
			"expected 2 endpoints, got %d",
			len(result.Endpoints),
		)
	}

	if strings.Contains(response.Body.String(), "secret-a") ||
		strings.Contains(response.Body.String(), "secret-b") {
		t.Error("response exposed endpoint secrets")
	}
}

func TestDisableEndpoint(t *testing.T) {
	var receivedID int64

	store := &fakeStore{
		disableEndpointFunc: func(
			ctx context.Context,
			endpointID int64,
		) error {
			receivedID = endpointID
			return nil
		},
	}

	server := New(store)

	request := httptest.NewRequest(
		http.MethodDelete,
		"/v1/endpoints/42",
		nil,
	)

	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf(
			"expected status %d, got %d; body=%s",
			http.StatusNoContent,
			response.Code,
			response.Body.String(),
		)
	}

	if receivedID != 42 {
		t.Errorf(
			"expected store ID 42, got %d",
			receivedID,
		)
	}

	if response.Body.Len() != 0 {
		t.Errorf(
			"expected empty response body, got %q",
			response.Body.String(),
		)
	}
}

func TestDisableEndpointNotFound(t *testing.T) {
	store := &fakeStore{
		disableEndpointFunc: func(
			ctx context.Context,
			endpointID int64,
		) error {
			return pgx.ErrNoRows
		},
	}

	server := New(store)

	request := httptest.NewRequest(
		http.MethodDelete,
		"/v1/endpoints/999",
		nil,
	)

	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf(
			"expected status %d, got %d; body=%s",
			http.StatusNotFound,
			response.Code,
			response.Body.String(),
		)
	}

	var result errorBody

	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatalf("decode error response: %v", err)
	}

	if result.Error.Code != "not_found" {
		t.Errorf(
			"expected error code %q, got %q",
			"not_found",
			result.Error.Code,
		)
	}
}

func TestCreateEndpointStoreError(t *testing.T) {
	store := &fakeStore{
		createEndpointFunc: func(
			ctx context.Context,
			url string,
			secret string,
		) (domain.Endpoint, error) {
			return domain.Endpoint{},
				errors.New("database connection failed")
		},
	}

	server := New(store)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/endpoints",
		strings.NewReader(`{
			"url":"https://receiver.example/webhook",
			"secret":"test-secret"
		}`),
	)
	request.Header.Set(
		"Content-Type",
		"application/json",
	)

	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusInternalServerError,
			response.Code,
		)
	}

	if strings.Contains(
		response.Body.String(),
		"database connection failed",
	) {
		t.Error("response leaked internal database error")
	}
}
