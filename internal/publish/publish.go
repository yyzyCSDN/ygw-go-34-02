// Package publish implements the publish pipeline: append to the task
// store, register, and enqueue ready tasks into the waiting queue.
package publish

import (
	"context"

	"jobsched/internal/deps"
	"jobsched/internal/metric"
	"jobsched/internal/model"
	"jobsched/internal/queue"
	"jobsched/internal/registry"
	"jobsched/internal/store"
)

// Broker owns the publish pipeline.
type Broker struct {
	store    *store.Store
	queue    *queue.Queue
	reg      *registry.Registry
	deps     *deps.Graph
	metrics  *metric.Metrics
	progress *store.Cursor
}

// New creates a broker over a store, queue, registry and dependency
// graph.
func New(s *store.Store, q *queue.Queue, reg *registry.Registry, g *deps.Graph, metrics *metric.Metrics) *Broker {
	return &Broker{
		store:    s,
		queue:    q,
		reg:      reg,
		deps:     g,
		metrics:  metrics,
		progress: store.NewCursor(s),
	}
}

// Publish appends, registers and enqueues a task, returning its
// sequence. Tasks with unsatisfied dependencies stay pending.
func (b *Broker) Publish(ctx context.Context, task model.Task) (int64, error) {
	seq, err := b.store.Append(task)
	if err != nil {
		return 0, err
	}
	task.Sequence = seq
	b.reg.Register(task)
	if b.deps.Ready(task.ID) {
		if err := b.queue.Enqueue(task); err != nil {
			return 0, err
		}
	}
	b.progress.Advance(seq)
	b.metrics.Published()
	return seq, nil
}

// Progress returns the highest published sequence.
func (b *Broker) Progress() int64 {
	return b.progress.Position()
}
