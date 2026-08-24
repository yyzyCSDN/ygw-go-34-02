package store

import (
	"testing"
	"time"

	"jobsched/internal/model"
)

func mustTime() time.Time {
	return time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
}

func TestStoreAppendCommitSince(t *testing.T) {
	s := NewStore(16)
	task := model.NewTask("batch/etl", []byte("hello"), mustTime())
	seq, err := s.Append(task)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Fatalf("first sequence = %d, want 1", seq)
	}
	if err := s.Commit(seq); err != nil {
		t.Fatal(err)
	}
	committed, err := s.Since(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(committed) != 1 || committed[0].Sequence != seq {
		t.Fatalf("committed = %v, want sequence %d", committed, seq)
	}
	if string(committed[0].Payload) != "hello" {
		t.Fatalf("payload = %q, want hello", committed[0].Payload)
	}
	if s.Len() != 1 {
		t.Fatalf("store length = %d, want 1", s.Len())
	}
}

func TestStoreRingFull(t *testing.T) {
	s := NewStore(2)
	for i := 0; i < 2; i++ {
		if _, err := s.Append(model.NewTask("g", []byte("x"), mustTime())); err != nil {
			t.Fatalf("append %d failed: %v", i+1, err)
		}
	}
	if _, err := s.Append(model.NewTask("g", []byte("x"), mustTime())); err == nil {
		t.Fatal("append beyond capacity should fail")
	}
}
