package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/hiroshi-os/fencepost/internal/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type AdminServer struct {
	store store.Store
	mux   *http.ServeMux
}

func NewAdminServer(st store.Store) *AdminServer {
	s := &AdminServer{
		store: st,
		mux:   http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *AdminServer) Handler() http.Handler {
	return s.mux
}

func (s *AdminServer) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.Handle("GET /metrics", promhttp.Handler())
	s.mux.HandleFunc("GET /admin/jobs/", s.handleJobTimeline)
}

func (s *AdminServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *AdminServer) handleJobTimeline(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/admin/jobs/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "missing job id", http.StatusBadRequest)
		return
	}

	jobID, err := uuid.Parse(parts[0])
	if err != nil {
		http.Error(w, "invalid job id uuid", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	job, err := s.store.Get(ctx, jobID)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	events, err := s.store.GetEvents(ctx, jobID)
	if err != nil {
		http.Error(w, "error retrieving events", http.StatusInternalServerError)
		return
	}

	resp := map[string]any{
		"job":    job,
		"events": events,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
