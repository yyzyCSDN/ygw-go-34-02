package model

import (
	"time"

	"github.com/google/uuid"
)

// ScheduleWindow describes one dispatch batch of due tasks.
type ScheduleWindow struct {
	ID        string
	OpenedAt  time.Time
	TaskCount int
}

// OpenWindow starts a new dispatch batch window.
func OpenWindow(count int) ScheduleWindow {
	return ScheduleWindow{
		ID:        uuid.NewString(),
		OpenedAt:  time.Now().UTC(),
		TaskCount: count,
	}
}

// ExecutorID is the stable identity of a worker.
type ExecutorID string
