package heartbeat

import (
	"testing"
	"time"

	"jobsched/internal/exec"
	"jobsched/internal/lease"
	"jobsched/internal/metric"
	"jobsched/internal/model"
)

func TestRenewActiveLease(t *testing.T) {
	leases := lease.New(time.Hour)
	execs := exec.NewManager(func(inst model.Instance) error { return nil }, 4, time.Second)
	metrics := metric.New()
	h := New(leases, execs, metrics, time.Hour, time.Hour)
	executor := model.ExecutorID("worker-1")
	execs.Add(executor)
	leases.Acquire(executor)
	if !h.Renew(executor) {
		t.Fatal("renewal of an active lease should succeed")
	}
	if leases.Count() != 1 {
		t.Fatalf("lease count = %d, want 1", leases.Count())
	}
}

func TestLeaseExpired(t *testing.T) {
	leases := lease.New(100 * time.Millisecond)
	execs := exec.NewManager(func(inst model.Instance) error { return nil }, 4, time.Second)
	metrics := metric.New()
	h := New(leases, execs, metrics, time.Hour, 100*time.Millisecond)
	executor := model.ExecutorID("worker-2")
	execs.Add(executor)
	leases.Acquire(executor)
	if leases.Expired(executor, time.Now().UTC()) {
		t.Fatal("fresh lease should not be expired")
	}
	if !leases.Expired(executor, time.Now().UTC().Add(2*time.Hour)) {
		t.Fatal("lease beyond timeout should be expired")
	}
	_ = h
}
