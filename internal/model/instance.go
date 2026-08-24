package model

import (
	"time"

	"github.com/google/uuid"
)

// InstanceState is the lifecycle state of one task execution.
type InstanceState int

const (
	// InstanceQueued means the execution is waiting to run.
	InstanceQueued InstanceState = iota
	// InstanceRunning means the execution is in progress.
	InstanceRunning
	// InstanceFailed means the execution failed and may be retried.
	InstanceFailed
	// InstanceSucceeded means the execution completed.
	InstanceSucceeded
)

// String renders the instance state.
func (s InstanceState) String() string {
	switch s {
	case InstanceQueued:
		return "queued"
	case InstanceRunning:
		return "running"
	case InstanceFailed:
		return "failed"
	case InstanceSucceeded:
		return "succeeded"
	default:
		return "unknown"
	}
}

// Instance is one execution attempt of a task.
type Instance struct {
	ID        string
	TaskID    string
	DedupeKey string
	State     InstanceState
	Attempt   int
	StartedAt time.Time
	EndedAt   time.Time
}

// NewInstance builds an execution attempt for a task.
func NewInstance(task Task, attempt int) Instance {
	return Instance{
		ID:        uuid.NewString(),
		TaskID:    task.ID,
		DedupeKey: task.DedupeKey,
		State:     InstanceQueued,
		Attempt:   attempt,
		StartedAt: time.Now().UTC(),
	}
}
