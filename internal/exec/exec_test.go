package exec

import (
	"errors"
	"sync"
	"testing"
	"time"

	"jobsched/internal/model"
)

func mustTime() time.Time {
	return time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
}

type recorder struct {
	mu      sync.Mutex
	records []string
}

func (r *recorder) add(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, id)
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.records)
}

func TestRunnerEnqueueExecutesOnce(t *testing.T) {
	rec := &recorder{}
	runner := NewRunner(func(inst model.Instance) error {
		rec.add(inst.ID)
		return nil
	}, 4, 100*time.Millisecond)
	task := model.NewTask("g", []byte("x"), mustTime())
	inst := model.NewInstance(task, 1)
	if err := runner.Enqueue(inst); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for rec.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if rec.count() != 1 {
		t.Fatalf("executions = %d, want 1", rec.count())
	}
	runner.Shutdown()
}

func TestManagerUnknownExecutor(t *testing.T) {
	m := NewManager(func(inst model.Instance) error { return nil }, 4, time.Second)
	if err := m.Dispatch(model.ExecutorID("missing"), model.Instance{}); err == nil {
		t.Fatal("dispatch to unknown executor should fail")
	}
}

func TestRunnerClosedRejectsEnqueue(t *testing.T) {
	runner := NewRunner(func(inst model.Instance) error { return nil }, 4, time.Second)
	runner.Shutdown()
	if err := runner.Enqueue(model.Instance{}); err == nil {
		t.Fatal("enqueue after shutdown should fail")
	}
}

var _ = errors.New
