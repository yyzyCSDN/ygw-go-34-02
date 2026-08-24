// Package retry plans retry instances for failed tasks.
package retry

import (
	"errors"
	"sync"

	"jobsched/internal/model"
)

// ErrTooManyRetries is returned when a task exceeds its retry budget.
var ErrTooManyRetries = errors.New("retry budget exhausted")

// Planner tracks attempts and produces retry instances.
type Planner struct {
	mu       sync.Mutex
	attempts map[string]int
	max      int
}

// New creates a retry planner.
func New(maxAttempts int) *Planner {
	return &Planner{attempts: make(map[string]int), max: maxAttempts}
}

// Validate rejects tasks that cannot be retried safely.
func (p *Planner) Validate(task model.Task) error {
	if task.ID == "" {
		return errors.New("task id is empty")
	}
	if task.DedupeKey == "" {
		return errors.New("task dedupe key is empty")
	}
	return nil
}

// Plan builds the next execution instance for a task, reusing the
// task's dedupe key so the executor can see it is the same work.
func (p *Planner) Plan(task model.Task) (model.Instance, error) {
	if err := p.Validate(task); err != nil {
		return model.Instance{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	attempt := p.attempts[task.ID] + 1
	if attempt > p.max {
		return model.Instance{}, ErrTooManyRetries
	}
	p.attempts[task.ID] = attempt
	inst := model.NewInstance(task, attempt)
	inst.DedupeKey = task.DedupeKey
	return inst, nil
}

// Attempts returns how many attempts a task has used.
func (p *Planner) Attempts(taskID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.attempts[taskID]
}

// Reset clears the attempt history of a task.
func (p *Planner) Reset(taskID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.attempts, taskID)
}
