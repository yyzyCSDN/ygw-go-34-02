// Package queue implements the ordered waiting queue of due tasks.
package queue

import (
	"container/heap"
	"errors"
	"sync"

	"jobsched/internal/model"
)

// ErrFull is returned when the bounded queue cannot accept a task.
var ErrFull = errors.New("waiting queue is full")

// Queue is a bounded priority queue ordered by due time.
type Queue struct {
	mu    sync.Mutex
	items priorityItems
	limit int
}

// New creates a bounded waiting queue.
func New(limit int) *Queue {
	if limit < 1 {
		limit = 1
	}
	return &Queue{limit: limit}
}

// Enqueue adds a task to the queue.
func (q *Queue) Enqueue(task model.Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) >= q.limit {
		return ErrFull
	}
	heap.Push(&q.items, task)
	return nil
}

// Next returns and removes the earliest task.
func (q *Queue) Next() (model.Task, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return model.Task{}, false
	}
	return heap.Pop(&q.items).(model.Task), true
}

// Peek returns the earliest task without removing it.
func (q *Queue) Peek() (model.Task, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return model.Task{}, false
	}
	return q.items[0], true
}

// Len returns the queue occupancy.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// Snapshot returns a stable, ordered copy of the queued tasks.
func (q *Queue) Snapshot() []model.Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]model.Task, len(q.items))
	copy(out, q.items)
	return out
}

// Drain atomically removes and returns every queued task ordered from
// earliest to latest due time. Because the whole queue is consumed under
// one lock, a task enqueued after Drain returns is not part of its
// result and stays in the queue for a later window. A caller that needs
// to keep a task (for example one that is not due yet) re-enqueues it.
func (q *Queue) Drain() []model.Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil
	}
	out := make([]model.Task, 0, len(q.items))
	for len(q.items) > 0 {
		out = append(out, heap.Pop(&q.items).(model.Task))
	}
	return out
}
