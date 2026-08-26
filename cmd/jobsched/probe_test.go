package main

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"testing"

	"jobsched"
	"jobsched/internal/deps"
	"jobsched/internal/dispatch"
	"jobsched/internal/exec"
	"jobsched/internal/flow"
	"jobsched/internal/heartbeat"
	"jobsched/internal/lease"
	"jobsched/internal/metric"
	"jobsched/internal/model"
	"jobsched/internal/publish"
	"jobsched/internal/queue"
	"jobsched/internal/registry"
	"jobsched/internal/store"
)

func newTestServer() *Server {
	cfg := Load("127.0.0.1:0")
	st := store.NewStore(32)
	q := queue.New(32)
	reg := registry.New()
	dg := deps.New()
	metrics := metric.New()
	execs := exec.NewManager(func(inst model.Instance) error { return nil }, 4, cfg.RunnerTimeout)
	leases := lease.New(cfg.LeaseTimeout)
	dispatcher := dispatch.New(q, st, reg, leases, execs, metrics, flow.NewLimiter(cfg.Flow))
	broker := publish.New(st, q, reg, dg, metrics)
	_ = heartbeat.NewEvictor(leases, execs, metrics)
	return NewServer(cfg, reg, dg, leases, execs, metrics, broker, dispatcher, st)
}

func TestProbeEndpoints(t *testing.T) {
	srv := newTestServer()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	if err := Probe(ts.URL); err != nil {
		t.Fatalf("probe failed: %v", err)
	}
}

func TestConsoleMarker(t *testing.T) {
	if !strings.Contains(string(web.ConsoleHTML), "JobScheduler Console") {
		t.Fatal("console marker missing")
	}
	if !bytes.Contains(web.ConsoleHTML, []byte("publish")) {
		t.Fatal("console page should reference publish")
	}
}
