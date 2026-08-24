package registry

import (
	"testing"
	"time"

	"jobsched/internal/model"
)

func mustTime() time.Time {
	return time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
}

func TestRegistryRegisterGetSetState(t *testing.T) {
	reg := New()
	task := model.NewTask("batch/etl", []byte("x"), mustTime())
	reg.Register(task)
	got, ok := reg.Get(task.ID)
	if !ok || got.ID != task.ID {
		t.Fatalf("task missing after register: %v", got.ID)
	}
	if reg.Version("batch/etl") == 0 {
		t.Fatal("group version should move on register")
	}
	reg.SetState(task.ID, model.TaskRunning)
	got, _ = reg.Get(task.ID)
	if got.State != model.TaskRunning {
		t.Fatalf("state = %v, want running", got.State)
	}
	if len(reg.List()) != 1 {
		t.Fatalf("list = %d, want 1", len(reg.List()))
	}
	if len(reg.Snapshot()) != 1 {
		t.Fatalf("snapshot = %d, want 1", len(reg.Snapshot()))
	}
}

func TestRegistryDeleteRemoves(t *testing.T) {
	reg := New()
	task := model.NewTask("g", []byte("x"), mustTime())
	reg.Register(task)
	reg.Delete(task.ID)
	if _, ok := reg.Get(task.ID); ok {
		t.Fatal("task should be gone after delete")
	}
}
