package model

import "testing"

func TestTaskStateString(t *testing.T) {
	cases := []struct {
		state TaskState
		want  string
	}{
		{TaskPending, "pending"},
		{TaskDue, "due"},
		{TaskDispatched, "dispatched"},
		{TaskRunning, "running"},
		{TaskDone, "done"},
		{TaskRemoved, "removed"},
	}
	for _, tc := range cases {
		if got := tc.state.String(); got != tc.want {
			t.Fatalf("state %d rendered %q, want %q", tc.state, got, tc.want)
		}
	}
}

func TestLeaseStateString(t *testing.T) {
	if got := LeaseEvicted.String(); got != "evicted" {
		t.Fatalf("unexpected lease state: %q", got)
	}
}

func TestNewTaskAndInstance(t *testing.T) {
	task := NewTask("batch/etl", []byte("x"), MustTime())
	if task.ID == "" || task.DedupeKey == "" {
		t.Fatal("task id or dedupe key is empty")
	}
	inst := NewInstance(task, 1)
	if inst.TaskID != task.ID || inst.DedupeKey != task.DedupeKey {
		t.Fatalf("instance did not inherit task identity: %+v", inst)
	}
	if inst.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", inst.Attempt)
	}
}

func TestScheduleWindowKey(t *testing.T) {
	w := OpenWindow(3)
	if w.TaskCount != 3 || w.ID == "" {
		t.Fatalf("unexpected window: %+v", w)
	}
}
