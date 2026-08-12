package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Ian747-tw/relayforge/internal/domain"
)

func TestHandleGetDelivery(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		getDelivery func(
			context.Context,
			int64,
		) (domain.DeliveryDetails, error)
		wantStatus int
		wantCode   string
	}{
		{
			name: "success",
			path: "/v1/deliveries/15",
			getDelivery: func(
				ctx context.Context,
				id int64,
			) (domain.DeliveryDetails, error) {
				if id != 15 {
					t.Fatalf("id = %d, want 15", id)
				}

				return domain.DeliveryDetails{
					ID:          15,
					EventID:     10,
					EndpointID:  3,
					Status:      "pending",
					EventType:   "user.created",
					Payload:     json.RawMessage(`{"user_id":123}`),
					EndpointURL: "https://example.com/webhook",
				}, nil
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "invalid ID",
			path: "/v1/deliveries/abc",
			getDelivery: func(
				context.Context,
				int64,
			) (domain.DeliveryDetails, error) {
				t.Fatal("store should not be called")
				return domain.DeliveryDetails{}, nil
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_id",
		},
		{
			name: "not found",
			path: "/v1/deliveries/999",
			getDelivery: func(
				context.Context,
				int64,
			) (domain.DeliveryDetails, error) {
				return domain.DeliveryDetails{}, domain.ErrNotFound
			},
			wantStatus: http.StatusNotFound,
			wantCode:   "not_found",
		},
		{
			name: "store failure",
			path: "/v1/deliveries/15",
			getDelivery: func(
				context.Context,
				int64,
			) (domain.DeliveryDetails, error) {
				return domain.DeliveryDetails{},
					errors.New("database unavailable")
			},
			wantStatus: http.StatusInternalServerError,
			wantCode:   "internal_error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{
				getDeliveryFunc: tt.getDelivery,
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

func TestHandleListDeliveryAttempts(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		listAttempts func(
			context.Context,
			int64,
		) ([]domain.DeliveryAttempt, error)
		wantStatus int
		wantBody   string
		wantCode   string
	}{
		{
			name: "success with attempts",
			path: "/v1/deliveries/15/attempts",
			listAttempts: func(
				ctx context.Context,
				deliveryID int64,
			) ([]domain.DeliveryAttempt, error) {
				if deliveryID != 15 {
					t.Fatalf(
						"delivery ID = %d, want 15",
						deliveryID,
					)
				}

				return []domain.DeliveryAttempt{
					{
						ID:            1,
						DeliveryID:    15,
						AttemptNumber: 1,
					},
				}, nil
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "existing delivery with no attempts",
			path: "/v1/deliveries/15/attempts",
			listAttempts: func(
				context.Context,
				int64,
			) ([]domain.DeliveryAttempt, error) {
				return []domain.DeliveryAttempt{}, nil
			},
			wantStatus: http.StatusOK,
			wantBody:   "[]",
		},
		{
			name: "invalid delivery ID",
			path: "/v1/deliveries/abc/attempts",
			listAttempts: func(
				context.Context,
				int64,
			) ([]domain.DeliveryAttempt, error) {
				t.Fatal("store should not be called")
				return nil, nil
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_id",
		},
		{
			name: "delivery not found",
			path: "/v1/deliveries/999/attempts",
			listAttempts: func(
				context.Context,
				int64,
			) ([]domain.DeliveryAttempt, error) {
				return nil, domain.ErrNotFound
			},
			wantStatus: http.StatusNotFound,
			wantCode:   "not_found",
		},
		{
			name: "store failure",
			path: "/v1/deliveries/15/attempts",
			listAttempts: func(
				context.Context,
				int64,
			) ([]domain.DeliveryAttempt, error) {
				return nil, errors.New("database unavailable")
			},
			wantStatus: http.StatusInternalServerError,
			wantCode:   "internal_error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{
				listDeliveryAttemptsFunc: tt.listAttempts,
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

			body := strings.TrimSpace(recorder.Body.String())

			if tt.wantBody != "" && body != tt.wantBody {
				t.Errorf(
					"body = %s, want %s",
					body,
					tt.wantBody,
				)
			}

			if tt.wantCode != "" &&
				!strings.Contains(body, tt.wantCode) {
				t.Errorf(
					"body = %s, want error code %q",
					body,
					tt.wantCode,
				)
			}
		})
	}
}
