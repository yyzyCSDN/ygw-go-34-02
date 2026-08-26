package registry_test

import (
	"testing"
	"time"

	"jobsched/internal/model"
	"jobsched/internal/registry"
)

func mustTime() time.Time {
	return time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
}

func TestReusedTaskIDDoesNotInheritExecution(t *testing.T) {
	reg := registry.New()
	first := model.NewTask("batch/etl", []byte("one"), mustTime())
	reg.Register(first)
	reg.SetState(first.ID, model.TaskDone)
	if reg.Incarnation(first.ID) != 1 {
		t.Fatalf("first incarnation = %d, want 1", reg.Incarnation(first.ID))
	}

	second := model.NewTask("batch/etl", []byte("two"), mustTime().Add(time.Minute))
	second.ID = first.ID
	second.DedupeKey = "fresh-key"
	reg.Register(second)

	got, ok := reg.Get(first.ID)
	if !ok {
		t.Fatal("task missing after re-register")
	}
	if got.State != model.TaskPending {
		t.Fatalf("re-registered task inherited old execution state %v, want pending", got.State)
	}
	if reg.Incarnation(first.ID) != 2 {
		t.Fatalf("re-register did not start a new incarnation, got %d", reg.Incarnation(first.ID))
	}
}
