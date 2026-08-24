package retry

import (
	"testing"
	"time"

	"jobsched/internal/model"
)

func mustTime() time.Time {
	return time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
}

func TestPlannerAttempts(t *testing.T) {
	p := New(2)
	task := model.NewTask("g", []byte("x"), mustTime())
	first, err := p.Plan(task)
	if err != nil {
		t.Fatal(err)
	}
	if first.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", first.Attempt)
	}
	second, err := p.Plan(task)
	if err != nil {
		t.Fatal(err)
	}
	if second.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", second.Attempt)
	}
	if _, err := p.Plan(task); err == nil {
		t.Fatal("attempt beyond budget should fail")
	}
	if p.Requeueable(task) {
		t.Fatal("exhausted task should not be requeueable")
	}
	p.Reset(task.ID)
	if p.Attempts(task.ID) != 0 {
		t.Fatal("reset should clear attempt history")
	}
}

func TestPlannerNextDue(t *testing.T) {
	p := New(1)
	task := model.NewTask("g", []byte("x"), mustTime())
	next := p.NextDue(task, model.OpenWindow(1))
	if next.State != model.TaskPending {
		t.Fatalf("state = %v, want pending", next.State)
	}
}
