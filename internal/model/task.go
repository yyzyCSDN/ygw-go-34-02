// Package model defines the shared domain types of the distributed job
// scheduler.
package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// TaskState is the lifecycle state of a scheduled task.
type TaskState int

const (
	// TaskPending means the task is waiting for its schedule window.
	TaskPending TaskState = iota
	// TaskDue means the task is inside its schedule window.
	TaskDue
	// TaskDispatched means the task was handed to an executor.
	TaskDispatched
	// TaskRunning means an executor is processing the task.
	TaskRunning
	// TaskDone means the task finished successfully.
	TaskDone
	// TaskRemoved means the task was deleted.
	TaskRemoved
)

// String renders the task state for the console.
func (s TaskState) String() string {
	switch s {
	case TaskPending:
		return "pending"
	case TaskDue:
		return "due"
	case TaskDispatched:
		return "dispatched"
	case TaskRunning:
		return "running"
	case TaskDone:
		return "done"
	case TaskRemoved:
		return "removed"
	default:
		return "unknown"
	}
}

// Task is one unit of scheduled work.
type Task struct {
	ID        string
	Sequence  int64
	Group     string
	Payload   []byte
	DedupeKey string
	DueAt     time.Time
	State     TaskState
	CreatedAt time.Time
}

// NewTask builds a task with a fresh id and dedupe key.
func NewTask(group string, payload []byte, dueAt time.Time) Task {
	return Task{
		ID:        uuid.NewString(),
		Group:     group,
		Payload:   payload,
		DedupeKey: uuid.NewString(),
		DueAt:     dueAt,
		State:     TaskPending,
		CreatedAt: time.Now().UTC(),
	}
}

// Key returns a stable identity used in logs and ordering assertions.
func (t Task) Key() string {
	return fmt.Sprintf("%s|%s", t.ID, t.Group)
}
