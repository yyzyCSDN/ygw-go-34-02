package publish

import (
	"context"
	"testing"
	"time"

	"jobsched/internal/deps"
	"jobsched/internal/metric"
	"jobsched/internal/model"
	"jobsched/internal/queue"
	"jobsched/internal/registry"
	"jobsched/internal/store"
)

func mustTime() time.Time {
	return time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
}

func TestPublishRegistersAndEnqueues(t *testing.T) {
	st := store.NewStore(16)
	q := queue.New(16)
	reg := registry.New()
	dg := deps.New()
	metrics := metric.New()
	b := New(st, q, reg, dg, metrics)
	seq, err := b.Publish(context.Background(), model.NewTask("batch/etl", []byte("x"), mustTime()))
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Fatalf("sequence = %d, want 1", seq)
	}
	tasks := reg.List()
	if len(tasks) != 1 {
		t.Fatalf("registry tasks = %d, want 1", len(tasks))
	}
	if q.Len() != 1 {
		t.Fatalf("queue len = %d, want 1", q.Len())
	}
	if metrics.Snapshot().Published != 1 {
		t.Fatalf("published metric = %d, want 1", metrics.Snapshot().Published)
	}
}

func TestPublishDependencyHoldsTask(t *testing.T) {
	st := store.NewStore(16)
	q := queue.New(16)
	reg := registry.New()
	dg := deps.New()
	metrics := metric.New()
	b := New(st, q, reg, dg, metrics)
	task := model.NewTask("g", []byte("x"), mustTime())
	dg.Add(task.ID, []string{"dep-1"})
	if _, err := b.Publish(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if q.Len() != 0 {
		t.Fatalf("task with pending dependency should stay out of queue")
	}
}
