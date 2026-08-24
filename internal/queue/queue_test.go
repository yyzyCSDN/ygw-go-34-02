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

func TestQueueDrainOrdersAndClears(t *testing.T) {
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
	got := q.Drain()
	if len(got) != 2 {
		t.Fatalf("drain len = %d, want 2", len(got))
	}
	if got[0].ID != first.ID || got[1].ID != second.ID {
		t.Fatalf("drain order = [%s, %s], want [%s, %s]", got[0].ID, got[1].ID, first.ID, second.ID)
	}
	if q.Len() != 0 {
		t.Fatalf("drain left %d task(s) behind", q.Len())
	}
}

func TestQueueDrainEmpty(t *testing.T) {
	q := New(8)
	if got := q.Drain(); got != nil {
		t.Fatalf("drain of empty queue = %v, want nil", got)
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
