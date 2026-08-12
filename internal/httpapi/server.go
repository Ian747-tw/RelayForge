package httpapi

import (
	"context"
	"net/http"

	"github.com/Ian747-tw/relayforge/internal/domain"
)

type Store interface {
	CreateEndpoint(
		context.Context,
		string,
		string,
	) (domain.Endpoint, error)

	ListActiveEndpoints(
		context.Context,
	) ([]domain.Endpoint, error)

	DisableEndpoint(
		context.Context,
		int64,
	) error

	CreateEventWithDeliveries(
		context.Context,
		domain.CreateEventParams,
	) (domain.Event, bool, error)

	GetEvent(
		context.Context,
		int64,
	) (domain.Event, error)

	GetDelivery(
		context.Context,
		int64,
	) (domain.DeliveryDetails, error)

	ListDeliveryAttempts(
		context.Context,
		int64,
	) ([]domain.DeliveryAttempt, error)
}

type Server struct {
	store Store
	mux   *http.ServeMux
}

func New(store Store) *Server {
	server := &Server{
		store: store,
		mux:   http.NewServeMux(),
	}

	server.registerRoutes()

	return server
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealth)

	s.mux.HandleFunc(
		"POST /v1/endpoints",
		s.handleCreateEndpoint,
	)

	s.mux.HandleFunc(
		"GET /v1/endpoints",
		s.handleListEndpoints,
	)

	s.mux.HandleFunc(
		"DELETE /v1/endpoints/{id}",
		s.handleDisableEndpoint,
	)

	s.mux.HandleFunc(
		"POST /v1/events",
		s.handleCreateEvent,
	)

	s.mux.HandleFunc(
		"GET /v1/events/{id}",
		s.handleGetEvent,
	)

	s.mux.HandleFunc(
		"GET /v1/deliveries/{id}",
		s.handleGetDelivery,
	)

	s.mux.HandleFunc(
		"GET /v1/deliveries/{id}/attempts",
		s.handleListDeliveryAttempts,
	)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}
