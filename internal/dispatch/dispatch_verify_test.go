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

type gateSource struct {
	started chan struct{}
	release chan struct{}
	tasks   []model.Task
}

func (g *gateSource) Since(after int64) ([]model.Task, error) {
	select {
	case g.started <- struct{}{}:
	default:
	}
	<-g.release
	return g.tasks, nil
}

type orderHandler struct {
	mu      sync.Mutex
	started chan struct{}
	release chan struct{}
	first   bool
	records []string
}

func newOrderHandler() *orderHandler {
	return &orderHandler{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		first:   true,
	}
}

func (h *orderHandler) handle(inst model.Instance) error {
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
	h.records = append(h.records, inst.TaskID)
	h.mu.Unlock()
	return nil
}

func (h *orderHandler) executed() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.records))
	copy(out, h.records)
	return out
}

func TestCatchupTasksOrderedBeforeLive(t *testing.T) {
	st := store.NewStore(32)
	q := queue.New(32)
	reg := registry.New()
	leases := lease.New(time.Minute)
	handler := newOrderHandler()
	execs := exec.NewManager(handler.handle, 16, time.Second)
	executor := model.ExecutorID("worker-1")
	execs.Add(executor)
	leases.Acquire(executor)
	metrics := metric.New()
	d := dispatch.New(q, st, reg, leases, execs, metrics, flow.NewLimiter(flow.DefaultPolicy()))

	missed := make([]model.Task, 5)
	for i := range missed {
		missed[i] = model.NewTask("batch/etl", []byte("old"), mustTime())
	}
	src := &gateSource{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		tasks:   missed,
	}

	catchupDone := make(chan error, 1)
	go func() {
		catchupDone <- d.CatchUpFrom(context.Background(), executor, 0, src)
	}()

	select {
	case <-src.started:
	case <-time.After(3 * time.Second):
		t.Fatal("catch-up did not start fetching")
	}

	live := model.NewTask("batch/etl", []byte("live"), mustTime())
	liveSeq, err := st.Append(live)
	if err != nil {
		t.Fatal(err)
	}
	live.Sequence = liveSeq
	reg.Register(live)
	if err := d.Dispatch(context.Background(), "batch/etl", []model.Task{live}); err != nil {
		t.Fatal(err)
	}

	close(src.release)
	select {
	case err := <-catchupDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("catch-up did not finish")
	}

	select {
	case <-handler.started:
	case <-time.After(3 * time.Second):
		t.Fatal("executor did not start")
	}
	close(handler.release)

	deadline := time.Now().Add(5 * time.Second)
	for len(handler.executed()) < 6 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	records := handler.executed()
	if len(records) != 6 {
		t.Fatalf("executed %d instances, want 6", len(records))
	}
	// All replayed (old) tasks must precede the live task.
	if records[5] != live.ID {
		t.Fatalf("live task must arrive after replayed tasks, order: %v", records)
	}
	for _, missedID := range []string{missed[0].ID, missed[1].ID, missed[2].ID, missed[3].ID, missed[4].ID} {
		found := false
		for i := 0; i < 5; i++ {
			if records[i] == missedID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("replayed task %s missing from leading positions: %v", missedID, records)
		}
	}
}
