package store

import (
	"sync"
)

// Cursor tracks one consumer's read position over committed tasks.
type Cursor struct {
	mu    sync.Mutex
	store *Store
	pos   int64
}

// NewCursor creates a cursor positioned before the first task.
func NewCursor(store *Store) *Cursor {
	return &Cursor{store: store}
}

// Advance moves the position forward; it never moves backwards.
func (c *Cursor) Advance(seq int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seq > c.pos {
		c.pos = seq
	}
}

// Position returns the current read position.
func (c *Cursor) Position() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pos
}
