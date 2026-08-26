package store

// Backlog describes pending work for one store.
type Backlog struct {
	Pending        int
	Committed      int64
	NextSeq        int64
	Retained       int
	Dispatched     int
	Retryable      int
	HighestPending int64
}

// BacklogInfo computes the current backlog of a store.
func BacklogInfo(s *Store) Backlog {
	var next int64
	s.mu.Lock()
	next = s.seq + 1
	s.mu.Unlock()
	if pending := s.Pending(); len(pending) > 0 {
		next = pending[0].Sequence
	}
	return Backlog{
		Pending:        len(s.Pending()),
		Committed:      s.Cursor(),
		NextSeq:        next,
		Retained:       s.Len(),
		Dispatched:     s.DispatchedCount(),
		Retryable:      s.UndispatchedPending(),
		HighestPending: s.HighestPending(),
	}
}
