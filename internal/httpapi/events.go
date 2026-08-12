package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Ian747-tw/relayforge/internal/domain"
)

type createEventRequest struct {
	EventType   string          `json:"event_type"`
	Payload     json.RawMessage `json:"payload"`
	EndpointIDs []int64         `json:"endpoint_ids"`
}

type eventResponse struct {
	ID             int64           `json:"id"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`
	CreatedAt      time.Time       `json:"created_at"`
	DeliveryCount  int             `json:"delivery_count"`
}

func makeEventResponse(
	event domain.Event,
	deliveryCount int,
) eventResponse {
	return eventResponse{
		ID:             event.ID,
		EventType:      event.EventType,
		Payload:        event.Payload,
		IdempotencyKey: event.IdempotencyKey,
		CreatedAt:      event.CreatedAt,
		DeliveryCount:  deliveryCount,
	}
}

func validatePayload(
	payload json.RawMessage,
) error {
	trimmed := bytes.TrimSpace(payload)

	if len(trimmed) == 0 {
		return errors.New("payload is required")
	}

	if bytes.Equal(trimmed, []byte("null")) {
		return errors.New("payload must not be null")
	}

	if !json.Valid(trimmed) {
		return errors.New("payload must contain valid JSON")
	}

	return nil
}

func validateCreateEventRequest(
	request createEventRequest,
	idempotencyKey string,
) error {
	if strings.TrimSpace(request.EventType) == "" {
		return errors.New("event_type is required")
	}

	if err := validatePayload(request.Payload); err != nil {
		return err
	}

	if strings.TrimSpace(idempotencyKey) == "" {
		return errors.New("idempotency_key is required")
	}

	if len([]byte(strings.TrimSpace(idempotencyKey))) > 255 {
		return errors.New("Key is at most 255 bytes")
	}

	if len(request.EndpointIDs) == 0 {
		return errors.New("endpoint_ids is required")
	}

	seen := make(map[int64]struct{}, len(request.EndpointIDs))

	for _, endpointID := range request.EndpointIDs {
		if endpointID <= 0 {
			return fmt.Errorf(
				"endpoint ID must be positive: %d",
				endpointID,
			)
		}

		if _, exists := seen[endpointID]; exists {
			return fmt.Errorf(
				"duplicate endpoint ID: %d",
				endpointID,
			)
		}

		seen[endpointID] = struct{}{}
	}

	return nil

}

func (s *Server) handleCreateEvent(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request createEventRequest

	if err := decodeJSON(w, r, &request); err != nil {
		writeRequestError(w, err)
		return
	}

	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))

	if err := validateCreateEventRequest(request, idempotencyKey); err != nil {
		_ = writeError(
			w,
			http.StatusBadRequest,
			"invalid_request",
			err.Error(),
		)
		return
	}

	params := domain.CreateEventParams{
		EventType:      request.EventType,
		Payload:        request.Payload,
		IdempotencyKey: idempotencyKey,
		EndpointIDs:    request.EndpointIDs,
	}

	event, created, err := s.store.CreateEventWithDeliveries(r.Context(), params)

	switch {
	case errors.Is(err, domain.ErrEndpointUnavailable):
		_ = writeError(
			w,
			http.StatusNotFound,
			"not_found",
			err.Error(),
		)
		return

	case errors.Is(err, domain.ErrIdempotencyConflict):
		_ = writeError(
			w,
			http.StatusConflict,
			"conflict",
			err.Error(),
		)
		return

	case err != nil:
		_ = writeError(
			w,
			http.StatusInternalServerError,
			"internal_error",
			"an internal error occurred",
		)
		return
	}

	w.Header().Set(
		"Location",
		fmt.Sprintf("/v1/events/%d", event.ID),
	)

	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}

	_ = writeJSON(
		w,
		status,
		makeEventResponse(
			event,
			len(request.EndpointIDs),
		),
	)

}

func parseID(r *http.Request) (int64, error) {
	rawID := r.PathValue("id")

	id, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil || id <= 0 {
		return 0, errors.New("ID must be a positive integer")
	}

	return id, nil
}

func (s *Server) handleGetEvent(
	w http.ResponseWriter,
	r *http.Request,
) {
	id, err := parseID(r)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"invalid_id",
			"event ID must be a positive integer",
		)
		return
	}

	event, err := s.store.GetEvent(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(
				w,
				http.StatusNotFound,
				"not_found",
				"event not found",
			)
			return
		}

		writeError(
			w,
			http.StatusInternalServerError,
			"internal_error",
			"internal server error",
		)
		return
	}

	if err := writeJSON(w, http.StatusOK, event); err != nil {
		// Log later when structured logging is introduced.
		return
	}
}

func (s *Server) handleGetDelivery(
	w http.ResponseWriter,
	r *http.Request,
) {
	id, err := parseID(r)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"invalid_id",
			"delivery ID must be a positive integer",
		)
		return
	}

	detail, err := s.store.GetDelivery(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(
				w,
				http.StatusNotFound,
				"not_found",
				"delivery not found",
			)
			return
		}

		writeError(
			w,
			http.StatusInternalServerError,
			"internal_error",
			"internal server error",
		)
		return
	}

	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) handleListDeliveryAttempts(
	w http.ResponseWriter,
	r *http.Request,
) {
	id, err := parseID(r)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"invalid_id",
			"delivery ID must be a positive integer",
		)
		return
	}

	attempts, err := s.store.ListDeliveryAttempts(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(
				w,
				http.StatusNotFound,
				"not_found",
				"delivery not found",
			)
			return
		}

		writeError(
			w,
			http.StatusInternalServerError,
			"internal_error",
			"internal server error",
		)
		return
	}

	writeJSON(w, http.StatusOK, attempts)
}
