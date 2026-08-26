// Package store implements the task store with a dispatch cursor.
package store

import (
	"errors"
	"sync"

	"jobsched/internal/model"
)

// ErrFull is returned when the bounded store cannot accept a task.
var ErrFull = errors.New("task store is full")

// ErrInvalidSequence is returned when a commit targets a sequence that
// was never appended.
var ErrInvalidSequence = errors.New("commit sequence was never appended")

// Store is a bounded, sequence-ordered task buffer. Append assigns the
// next sequence and leaves the task pending; Commit advances the cursor
// only after dispatch confirmed the hand-off.
type Store struct {
	mu         sync.Mutex
	ring       *Ring
	seq        int64
	committed  int64
	dispatched map[int64]struct{}
}

// NewStore creates a store with the given capacity.
func NewStore(capacity int) *Store {
	return &Store{
		ring:       NewRing(capacity),
		dispatched: make(map[int64]struct{}),
	}
}

// Append assigns the next sequence and stores the task as pending.
func (s *Store) Append(task model.Task) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	task.Sequence = s.seq
	if !s.ring.Push(task) {
		return 0, ErrFull
	}
	return s.seq, nil
}

// Commit advances the committed cursor to seq.
func (s *Store) Commit(seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq > s.seq {
		return ErrInvalidSequence
	}
	if seq > s.committed {
		s.committed = seq
	}
	return nil
}

// Cursor returns the highest committed sequence.
func (s *Store) Cursor() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committed
}

// Since returns committed tasks with a sequence greater than after.
func (s *Store) Since(after int64) ([]model.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []model.Task
	for seq := after + 1; seq <= s.committed; seq++ {
		if task, ok := s.ring.Get(seq); ok {
			out = append(out, task)
		}
	}
	return out, nil
}

// Pending returns appended tasks that were not yet committed.
func (s *Store) Pending() []model.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []model.Task
	s.ring.Each(func(task model.Task) bool {
		if task.Sequence > s.committed {
			out = append(out, task)
		}
		return true
	})
	return out
}

// Len returns how many tasks are currently retained.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ring.Len()
}

// MarkDispatched records a task sequence as handed off.
func (s *Store) MarkDispatched(seq int64, dispatched bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq > s.seq {
		return
	}
	if dispatched {
		s.dispatched[seq] = struct{}{}
		return
	}
	delete(s.dispatched, seq)
}

// MarkDispatchedBatch records a batch of sequences as handed off. It is
// used after a dispatch round completes so every successful hand-off is
// committed consistently.
func (s *Store) MarkDispatchedBatch(seqs []int64, dispatched bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seq := range seqs {
		if seq > s.seq {
			continue
		}
		if dispatched {
			s.dispatched[seq] = struct{}{}
		} else {
			delete(s.dispatched, seq)
		}
	}
}

// Dispatched reports whether a sequence was marked as handed off.
func (s *Store) Dispatched(seq int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.dispatched[seq]
	return ok
}

// DispatchedCount returns how many tasks are marked dispatched.
func (s *Store) DispatchedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.dispatched)
}

// UndispatchedPending returns how many pending tasks are not marked as
// dispatched and therefore remain retryable.
func (s *Store) UndispatchedPending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	s.ring.Each(func(task model.Task) bool {
		if task.Sequence > s.committed {
			if _, ok := s.dispatched[task.Sequence]; !ok {
				count++
			}
		}
		return true
	})
	return count
}

// HighestPending returns the highest pending sequence, or zero.
func (s *Store) HighestPending() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var highest int64
	s.ring.Each(func(task model.Task) bool {
		if task.Sequence > s.committed && task.Sequence > highest {
			highest = task.Sequence
		}
		return true
	})
	return highest
}
