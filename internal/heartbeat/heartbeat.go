// Package heartbeat keeps executor leases alive and reclaims stale ones.
package heartbeat

import (
	"context"
	"time"

	"jobsched/internal/exec"
	"jobsched/internal/lease"
	"jobsched/internal/metric"
	"jobsched/internal/model"
)

// Heartbeat scans the lease table on a fixed interval.
type Heartbeat struct {
	leases   *lease.Manager
	execs    *exec.Manager
	metrics  *metric.Metrics
	interval time.Duration
	timeout  time.Duration
	evictor  *Evictor
}

// New creates a heartbeat controller.
func New(leases *lease.Manager, execs *exec.Manager, metrics *metric.Metrics, interval, timeout time.Duration) *Heartbeat {
	return &Heartbeat{
		leases:   leases,
		execs:    execs,
		metrics:  metrics,
		interval: interval,
		timeout:  timeout,
		evictor:  NewEvictor(leases, execs, metrics),
	}
}

// Run performs periodic sweeps until the context is cancelled.
func (h *Heartbeat) Run(ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			h.sweep(now)
		}
	}
}

func (h *Heartbeat) sweep(now time.Time) {
	for _, exec := range h.leases.ExpiredLeases(now) {
		h.evictor.Evict(exec)
	}
	for _, exec := range h.leases.Executors() {
		if h.expired(exec, now) {
			continue
		}
		_ = h.Renew(exec)
	}
}

// expired reports whether an executor lease has lapsed.
func (h *Heartbeat) expired(exec model.ExecutorID, now time.Time) bool {
	return h.leases.Expired(exec, now)
}

// Renew refreshes the lease of an executor.
func (h *Heartbeat) Renew(exec model.ExecutorID) bool {
	return h.leases.Renew(exec)
}

// Evictor exposes the eviction entry point.
func (h *Heartbeat) Evictor() *Evictor {
	return h.evictor
}
