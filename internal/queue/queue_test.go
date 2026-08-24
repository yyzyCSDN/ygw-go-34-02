package queue

import (
	"testing"
	"time"

	"jobsched/internal/model"
)

func mustTime() time.Time {
	return time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
}

func TestQueueOrderedNext(t *testing.T) {
	q := New(8)
	base := mustTime()
	first := model.NewTask("g", []byte("1"), base)
	second := model.NewTask("g", []byte("2"), base.Add(1))
	if err := q.Enqueue(second); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(first); err != nil {
		t.Fatal(err)
	}
	if q.Len() != 2 {
		t.Fatalf("len = %d, want 2", q.Len())
	}
	got, ok := q.Next()
	if !ok || got.ID != first.ID {
		t.Fatalf("earliest task = %v, want %s", got.ID, first.ID)
	}
	got, ok = q.Next()
	if !ok || got.ID != second.ID {
		t.Fatalf("second task = %v, want %s", got.ID, second.ID)
	}
	if _, ok := q.Next(); ok {
		t.Fatal("queue should be empty")
	}
}

func TestQueuePeekAndSnapshot(t *testing.T) {
	q := New(4)
	base := mustTime()
	task := model.NewTask("g", []byte("x"), base)
	if err := q.Enqueue(task); err != nil {
		t.Fatal(err)
	}
	peeked, ok := q.Peek()
	if !ok || peeked.ID != task.ID {
		t.Fatalf("peek = %v, want %s", peeked.ID, task.ID)
	}
	if q.Len() != 1 {
		t.Fatalf("peek changed queue: len=%d", q.Len())
	}
	if got := len(q.Snapshot()); got != 1 {
		t.Fatalf("snapshot len = %d, want 1", got)
	}
}

func TestQueueFull(t *testing.T) {
	q := New(1)
	base := mustTime()
	if err := q.Enqueue(model.NewTask("g", []byte("1"), base)); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(model.NewTask("g", []byte("2"), base)); err == nil {
		t.Fatal("enqueue beyond capacity should fail")
	}
}
