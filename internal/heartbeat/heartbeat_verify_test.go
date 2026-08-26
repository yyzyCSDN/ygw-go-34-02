package heartbeat_test

import (
	"sync"
	"testing"
	"time"

	"jobsched/internal/exec"
	"jobsched/internal/heartbeat"
	"jobsched/internal/lease"
	"jobsched/internal/metric"
	"jobsched/internal/model"
)

type gatingHandler struct {
	mu      sync.Mutex
	started chan struct{}
	release chan struct{}
	first   bool
}

func newGatingHandler() *gatingHandler {
	return &gatingHandler{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		first:   true,
	}
}

func (h *gatingHandler) handle(inst model.Instance) error {
	h.mu.Lock()
	first := h.first
	if first {
		h.first = false
	}
	h.mu.Unlock()
	if first {
		select {
		case h.started <- struct{}{}:
		default:
		}
		<-h.release
	}
	return nil
}

func TestRenewalCannotReviveEvictedLease(t *testing.T) {
	leases := lease.New(time.Minute)
	handler := newGatingHandler()
	execs := exec.NewManager(handler.handle, 4, time.Second)
	metrics := metric.New()
	h := heartbeat.New(leases, execs, metrics, time.Minute, time.Minute)

	executor := model.ExecutorID("worker-1")
	execs.Add(executor)
	leases.Acquire(executor)

	task := model.NewTask("batch/etl", []byte("x"), time.Now().UTC())
	inst := model.NewInstance(task, 1)
	inst.DedupeKey = task.DedupeKey
	if err := execs.Dispatch(executor, inst); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handler.started:
	case <-time.After(3 * time.Second):
		t.Fatal("task did not start executing")
	}

	evictDone := make(chan struct{})
	go func() {
		h.Evictor().Evict(executor)
		close(evictDone)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if l, ok := leases.Lease(executor); ok && l.State == model.LeaseEvicted {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	l, ok := leases.Lease(executor)
	if !ok || l.State != model.LeaseEvicted {
		t.Fatal("eviction did not reach the evicted state")
	}
	if h.Renew(executor) {
		t.Fatal("renewal revived a lease that is being evicted")
	}

	close(handler.release)
	select {
	case <-evictDone:
	case <-time.After(10 * time.Second):
		t.Fatal("eviction did not finish")
	}
	if l, ok := leases.Lease(executor); !ok || l.State != model.LeaseEvicted {
		t.Fatalf("lease state = %v, want evicted", l.State)
	}
}
