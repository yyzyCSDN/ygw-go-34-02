package store

import "jobsched/internal/model"

// Ring is the fixed-capacity circular buffer backing the store.
type Ring struct {
	buf   []model.Task
	head  int
	count int
	cap   int
}

// NewRing creates a circular buffer with the given capacity.
func NewRing(capacity int) *Ring {
	if capacity < 1 {
		capacity = 1
	}
	return &Ring{
		buf: make([]model.Task, capacity),
		cap: capacity,
	}
}

// Push appends a task, reporting false when the buffer is full.
func (r *Ring) Push(task model.Task) bool {
	if r.count == r.cap {
		return false
	}
	r.buf[(r.head+r.count)%r.cap] = task
	r.count++
	return true
}

// Get returns the task with the exact sequence, if retained.
func (r *Ring) Get(seq int64) (model.Task, bool) {
	for i := 0; i < r.count; i++ {
		task := r.buf[(r.head+i)%r.cap]
		if task.Sequence == seq {
			return task, true
		}
	}
	return model.Task{}, false
}

// Each visits every retained task in insertion order.
func (r *Ring) Each(fn func(model.Task) bool) {
	for i := 0; i < r.count; i++ {
		if !fn(r.buf[(r.head+i)%r.cap]) {
			return
		}
	}
}

// Len returns the number of retained tasks.
func (r *Ring) Len() int {
	return r.count
}
