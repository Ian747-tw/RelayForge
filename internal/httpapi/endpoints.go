package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Ian747-tw/relayforge/internal/domain"
	"github.com/jackc/pgx/v5"
)

const maxRequestBodyBytes int64 = 1 << 20 // 1 MiB

type createEndpointRequest struct {
	URL    string `json:"url"`
	Secret string `json:"secret"`
}

type endpointResponse struct {
	ID         int64      `json:"id"`
	URL        string     `json:"url"`
	CreatedAt  time.Time  `json:"created_at"`
	DisabledAt *time.Time `json:"disabled_at"`
}

type listEndpointsResponse struct {
	Endpoints []endpointResponse `json:"endpoints"`
}

type errorBody struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type requestError struct {
	Status  int
	Code    string
	Message string
}

func (e *requestError) Error() string {
	return e.Message
}

func newRequestError(
	status int,
	code string,
	message string,
) error {
	return &requestError{
		Status:  status,
		Code:    code,
		Message: message,
	}
}

func makeEndpointResponse(
	endpoint domain.Endpoint,
) endpointResponse {
	return endpointResponse{
		ID:         endpoint.ID,
		URL:        endpoint.URL,
		CreatedAt:  endpoint.CreatedAt,
		DisabledAt: endpoint.DisabledAt,
	}
}

func writeJSON(
	w http.ResponseWriter,
	status int,
	value any,
) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal JSON response: %w", err)
	}

	data = append(data, '\n')

	w.Header().Set(
		"Content-Type",
		"application/json; charset=utf-8",
	)
	w.Header().Set(
		"X-Content-Type-Options",
		"nosniff",
	)

	w.WriteHeader(status)

	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write JSON response: %w", err)
	}

	return nil

}

func writeError(
	w http.ResponseWriter,
	status int,
	code string,
	message string,
) error {
	return writeJSON(
		w,
		status,
		errorBody{
			Error: apiError{
				Code:    code,
				Message: message,
			},
		},
	)
}

func writeRequestError(
	w http.ResponseWriter,
	err error,
) {
	var requestErr *requestError

	if errors.As(err, &requestErr) {
		_ = writeError(
			w,
			requestErr.Status,
			requestErr.Code,
			requestErr.Message,
		)
		return
	}

	_ = writeError(
		w,
		http.StatusInternalServerError,
		"internal_error",
		"an internal error occurred",
	)
}

func decodeJSON(
	w http.ResponseWriter,
	r *http.Request,
	destination any,
) error {
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		return newRequestError(
			http.StatusBadRequest,
			"missing_content_type",
			"Content-Type header not found",
		)
	}

	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType != "application/json" {
		return newRequestError(
			http.StatusUnsupportedMediaType,
			"unsupported_media_type",
			"Content-Type must be application/json",
		)
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(destination); err != nil {
		var maxBytesErr *http.MaxBytesError

		if errors.As(err, &maxBytesErr) {
			return newRequestError(
				http.StatusRequestEntityTooLarge,
				"request_too_large",
				fmt.Sprintf("request body must not exceed %d", maxRequestBodyBytes),
			)
		}

		if strings.HasPrefix(err.Error(), "json: unknown field") {
			field := strings.TrimPrefix(
				err.Error(),
				"json: unknown field ",
			)

			return newRequestError(
				http.StatusBadRequest,
				"unknown_field",
				fmt.Sprintf(
					"request body contains unknown field %s",
					field,
				),
			)
		}

		return newRequestError(
			http.StatusBadRequest,
			"invalid_json",
			"request body contains invalid json",
		)
	}

	if decoder.More() {
		return newRequestError(
			http.StatusBadRequest,
			"invalid_json",
			"Request Body must contain only a single JSON object",
		)
	}

	var trailing struct{}
	err = decoder.Decode(&trailing)
	if err != nil && !errors.Is(err, io.EOF) {
		return newRequestError(
			http.StatusBadRequest,
			"invalid_json",
			"Request body contains trailing garbage data",
		)
	}

	return nil

}

func validateCreateEndpointRequest(
	request createEndpointRequest,
) error {
	if request.URL == "" {
		return errors.New("URL is empty")
	}

	if request.Secret == "" {
		return errors.New("Secret is empty")
	}

	parsedURL, err := url.Parse(request.URL)
	if err != nil {
		return fmt.Errorf("Error parsing URL: %v", err)
	}

	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return errors.New("unsupported url scheme")
	}

	if parsedURL.Host == "" {
		return errors.New("url host is empty")
	}

	if !parsedURL.IsAbs() {
		return errors.New("url is not absolute")
	}

	return nil
}

func (s *Server) handleCreateEndpoint(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request createEndpointRequest

	if err := decodeJSON(w, r, &request); err != nil {
		writeRequestError(w, err)
		return
	}

	if err := validateCreateEndpointRequest(request); err != nil {
		_ = writeError(
			w,
			http.StatusBadRequest,
			"invalid request",
			err.Error(),
		)
		return
	}

	endpoint, err := s.store.CreateEndpoint(
		r.Context(),
		request.URL,
		request.Secret,
	)

	if err != nil {
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
		fmt.Sprintf("/v1/endpoints/%d", endpoint.ID),
	)

	_ = writeJSON(
		w,
		http.StatusCreated,
		makeEndpointResponse(endpoint),
	)
}

func (s *Server) handleListEndpoints(
	w http.ResponseWriter,
	r *http.Request,
) {
	endpoints, err := s.store.ListActiveEndpoints(r.Context())
	if err != nil {
		_ = writeError(
			w,
			http.StatusInternalServerError,
			"internal_error",
			"an internal error occurred",
		)
		return
	}

	listed_endpoints := listEndpointsResponse{
		Endpoints: []endpointResponse{},
	}

	for _, endpoint := range endpoints {
		listed_endpoints.Endpoints = append(listed_endpoints.Endpoints, makeEndpointResponse(endpoint))
	}

	_ = writeJSON(
		w,
		http.StatusOK,
		listed_endpoints,
	)

}

func (s *Server) handleDisableEndpoint(
	w http.ResponseWriter,
	r *http.Request,
) {
	endpointID, err := strconv.ParseInt(
		r.PathValue("id"),
		10,
		64,
	)

	if err != nil {
		_ = writeError(
			w,
			http.StatusBadRequest,
			"invalid_request_var",
			fmt.Sprintf("error parsing id: %v", err),
		)
		return
	}

	err = s.store.DisableEndpoint(r.Context(), endpointID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		_ = writeError(
			w,
			http.StatusNotFound,
			"not_found",
			"id not found",
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
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}
