package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"jobsched"
	"jobsched/internal/deps"
	"jobsched/internal/dispatch"
	"jobsched/internal/exec"
	"jobsched/internal/lease"
	"jobsched/internal/metric"
	"jobsched/internal/model"
	"jobsched/internal/publish"
	"jobsched/internal/registry"
	"jobsched/internal/store"
)

// Server wires the HTTP endpoints, publish API and executor endpoint.
type Server struct {
	cfg        Config
	reg        *registry.Registry
	deps       *deps.Graph
	leases     *lease.Manager
	execs      *exec.Manager
	metrics    *metric.Metrics
	broker     *publish.Broker
	dispatcher *dispatch.Dispatcher
	store      *store.Store
	httpSrv    *http.Server
}

// NewServer builds the scheduler server.
func NewServer(
	cfg Config,
	reg *registry.Registry,
	deps *deps.Graph,
	leases *lease.Manager,
	execs *exec.Manager,
	metrics *metric.Metrics,
	broker *publish.Broker,
	dispatcher *dispatch.Dispatcher,
	store *store.Store,
) *Server {
	return &Server{
		cfg:        cfg,
		reg:        reg,
		deps:       deps,
		leases:     leases,
		execs:      execs,
		metrics:    metrics,
		broker:     broker,
		dispatcher: dispatcher,
		store:      store,
	}
}

// Handler returns the HTTP routing table.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/publish", s.handlePublish)
	mux.HandleFunc("/executors", s.handleExecutor)
	mux.HandleFunc("/", s.handleConsole)
	return mux
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe(addr string) error {
	s.httpSrv = &http.Server{Addr: addr, Handler: s.Handler()}
	return s.httpSrv.ListenAndServe()
}

// Shutdown stops the HTTP server gracefully.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpSrv == nil {
		return nil
	}
	return s.httpSrv.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	hits, misses := s.dispatcher.CacheStats()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"tasks":        s.reg.Snapshot(),
		"executors":    s.leases.Executors(),
		"lease_count":  s.leases.Count(),
		"metrics":      s.metrics.Snapshot(),
		"backlog":      store.BacklogInfo(s.store),
		"cache_hits":   hits,
		"cache_misses": misses,
		"progress":     s.broker.Progress(),
	})
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Group     string   `json:"group"`
		Payload   string   `json:"payload"`
		DueAfter  int64    `json:"due_after_ms"`
		DependsOn []string `json:"depends_on"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Group) == "" {
		http.Error(w, "group is required", http.StatusBadRequest)
		return
	}
	task := model.NewTask(req.Group, []byte(req.Payload), time.Now().UTC().Add(time.Duration(req.DueAfter)*time.Millisecond))
	for _, dep := range req.DependsOn {
		s.deps.Add(task.ID, []string{dep})
	}
	seq, err := s.broker.Publish(r.Context(), task)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{"sequence": seq, "task_id": task.ID, "status": "accepted"})
}

func (s *Server) handleExecutor(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	executor := model.ExecutorID(req.ID)
	if executor == "" {
		http.Error(w, "executor id is required", http.StatusBadRequest)
		return
	}
	s.execs.Add(executor)
	slot, _ := s.leases.Acquire(executor)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{"executor": executor, "slot": slot, "status": "registered"})
}

func (s *Server) handleConsole(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(web.ConsoleHTML)
}

var _ = log.Printf
