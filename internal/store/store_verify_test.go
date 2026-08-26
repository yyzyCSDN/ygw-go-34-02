package store_test

import (
	"testing"
	"time"

	"jobsched/internal/model"
	"jobsched/internal/store"
)

func mustTime() time.Time {
	return time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
}

func TestCursorAdvancesAfterDispatchSuccess(t *testing.T) {
	s := store.NewStore(16)
	task := model.NewTask("batch/etl", []byte("x"), mustTime())
	if _, err := s.Append(task); err != nil {
		t.Fatal(err)
	}
	if cur := s.Cursor(); cur != 0 {
		t.Fatalf("cursor advanced to %d before dispatch succeeded", cur)
	}
	if pending := len(s.Pending()); pending != 1 {
		t.Fatalf("failed dispatch left %d pending tasks, want 1 retryable", pending)
	}
}
