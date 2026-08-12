package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealth(t *testing.T) {
	server := New(nil)
	request := httptest.NewRequest(
		http.MethodGet,
		"/healthz",
		nil,
	)

	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusOK,
			response.Code,
		)
	}

	contentType := response.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf(
			"expected Content-Type application/json, got %q",
			contentType,
		)
	}

	var body struct {
		Status string `json:"status"`
	}

	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v,", err)
	}

	if body.Status != "ok" {
		t.Errorf(
			"expected status body: %q, got %q",
			"ok",
			body.Status,
		)
	}
}

func TestHealthRejectsPost(t *testing.T) {
	server := New(nil)

	request := httptest.NewRequest(
		http.MethodPost,
		"/healthz",
		nil,
	)

	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusMethodNotAllowed,
			response.Code,
		)
	}
}
