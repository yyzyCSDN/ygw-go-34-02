package metric

import "sync/atomic"

// Counters is a set of atomic counters.
type Counters struct {
	value int64
}

// NewCounters creates a zeroed counter.
func NewCounters() *Counters { return &Counters{} }

// Inc atomically increments the counter.
func (c *Counters) Inc() {
	atomic.AddInt64(&c.value, 1)
}

// Value returns the current value.
func (c *Counters) Value() int64 {
	return atomic.LoadInt64(&c.value)
}
