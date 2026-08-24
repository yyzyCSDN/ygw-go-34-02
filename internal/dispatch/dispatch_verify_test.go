package dispatch_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"jobsched/internal/dispatch"
	"jobsched/internal/exec"
	"jobsched/internal/flow"
	"jobsched/internal/lease"
	"jobsched/internal/metric"
	"jobsched/internal/model"
	"jobsched/internal/queue"
	"jobsched/internal/registry"
	"jobsched/internal/store"
)

func mustTime() time.Time {
	return time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
}

type recordHandler struct {
	mu      sync.Mutex
	records []string
}

func (h *recordHandler) handle(inst model.Instance) error {
	h.mu.Lock()
	h.records = append(h.records, inst.TaskID)
	h.mu.Unlock()
	return nil
}

func (h *recordHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.records)
}

func TestDispatchUsesFreshRegistryAfterDelete(t *testing.T) {
	st := store.NewStore(32)
	q := queue.New(32)
	reg := registry.New()
	leases := lease.New(time.Minute)
	handler := &recordHandler{}
	execs := exec.NewManager(handler.handle, 8, time.Second)
	executor := model.ExecutorID("worker-1")
	execs.Add(executor)
	leases.Acquire(executor)
	metrics := metric.New()
	d := dispatch.New(q, st, reg, leases, execs, metrics, flow.NewLimiter(flow.DefaultPolicy()))

	task := model.NewTask("batch/etl", []byte("x"), mustTime())
	reg.Register(task)
	if err := d.Dispatch(context.Background(), "batch/etl", []model.Task{task}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for handler.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if handler.count() != 1 {
		t.Fatalf("first dispatch executed %d times, want 1", handler.count())
	}

	reg.Delete(task.ID)
	if err := d.Dispatch(context.Background(), "batch/etl", []model.Task{task}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if handler.count() != 1 {
		t.Fatalf("deleted task was dispatched %d times, want only the pre-delete execution", handler.count())
	}
}
