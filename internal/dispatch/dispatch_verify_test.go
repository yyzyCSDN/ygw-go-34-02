package dispatch_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"jobsched/internal/clock"
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

type gateHandler struct {
	mu      sync.Mutex
	started chan struct{}
	release chan struct{}
	first   bool
	records []string
}

func newGateHandler() *gateHandler {
	return &gateHandler{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		first:   true,
	}
}

func (h *gateHandler) handle(inst model.Instance) error {
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
	h.mu.Lock()
	h.records = append(h.records, inst.ID)
	h.mu.Unlock()
	return nil
}

func (h *gateHandler) executed() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.records))
	copy(out, h.records)
	return out
}

func TestDispatchWindowSkipsMidBatchEnqueue(t *testing.T) {
	st := store.NewStore(32)
	q := queue.New(32)
	reg := registry.New()
	leases := lease.New(time.Minute)
	handler := newGateHandler()
	execs := exec.NewManager(handler.handle, 1, 5*time.Second)
	executor := model.ExecutorID("worker-1")
	execs.Add(executor)
	leases.Acquire(executor)
	metrics := metric.New()
	d := dispatch.New(q, st, reg, leases, execs, metrics, flow.NewLimiter(flow.DefaultPolicy()))

	base := mustTime()
	win := clock.Window{Begin: base, End: base.Add(time.Minute)}
	tasks := make([]model.Task, 5)
	for i := range tasks {
		tasks[i] = model.NewTask("batch/etl", []byte("x"), base.Add(time.Duration(i)*time.Millisecond))
		reg.Register(tasks[i])
		if err := q.Enqueue(tasks[i]); err != nil {
			t.Fatal(err)
		}
	}

	done := make(chan error, 1)
	go func() {
		done <- d.Tick(context.Background(), win)
	}()

	select {
	case <-handler.started:
	case <-time.After(3 * time.Second):
		t.Fatal("dispatch did not start executing")
	}

	// A task becomes due mid-batch; it must wait for the next window.
	late := model.NewTask("batch/etl", []byte("late"), base.Add(10*time.Millisecond))
	if err := q.Enqueue(late); err != nil {
		t.Fatal(err)
	}
	close(handler.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("tick did not finish")
	}

	deadline := time.Now().Add(2 * time.Second)
	for len(handler.executed()) < 5 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if q.Len() != 1 {
		t.Fatalf("mid-batch task should stay queued for the next window, queue len = %d", q.Len())
	}
	for _, id := range handler.executed() {
		if id == late.ID {
			t.Fatalf("mid-batch task %s was dispatched inside the current batch", id)
		}
	}
}
