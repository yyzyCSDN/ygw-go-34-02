package exec

import (
	"errors"
	"sync"
	"time"

	"jobsched/internal/model"
)

// ErrNoExecutor is returned when a task targets an unknown executor.
var ErrNoExecutor = errors.New("unknown executor")

// Manager owns the runner for each executor.
type Manager struct {
	mu        sync.Mutex
	runners   map[model.ExecutorID]*Runner
	handler   Handler
	queueSize int
	timeout   time.Duration
}

// NewManager creates an executor manager.
func NewManager(handler Handler, queueSize int, timeout time.Duration) *Manager {
	return &Manager{
		runners:   make(map[model.ExecutorID]*Runner),
		handler:   handler,
		queueSize: queueSize,
		timeout:   timeout,
	}
}

// Add registers an executor runner.
func (m *Manager) Add(exec model.ExecutorID) *Runner {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.runners[exec]; ok {
		return r
	}
	r := NewRunner(m.handler, m.queueSize, m.timeout)
	m.runners[exec] = r
	return r
}

// Get returns the runner of an executor.
func (m *Manager) Get(exec model.ExecutorID) (*Runner, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.runners[exec]
	return r, ok
}

// Remove drops the runner of an executor.
func (m *Manager) Remove(exec model.ExecutorID) {
	m.mu.Lock()
	delete(m.runners, exec)
	m.mu.Unlock()
}

// Dispatch hands an instance to an executor through its runner.
func (m *Manager) Dispatch(exec model.ExecutorID, inst model.Instance) error {
	r, ok := m.Get(exec)
	if !ok {
		return ErrNoExecutor
	}
	return r.Enqueue(inst)
}

// BeginReplay opens the replay window for an executor so concurrent live
// dispatches buffer behind the replayed tasks.
func (m *Manager) BeginReplay(exec model.ExecutorID) (bool, error) {
	r, ok := m.Get(exec)
	if !ok {
		return false, ErrNoExecutor
	}
	return r.BeginReplay(), nil
}

// ReplayWrite queues a replayed instance directly, ahead of buffered live
// dispatches.
func (m *Manager) ReplayWrite(exec model.ExecutorID, inst model.Instance) error {
	r, ok := m.Get(exec)
	if !ok {
		return ErrNoExecutor
	}
	return r.ReplayWrite(inst)
}

// EndReplay closes the replay window and flushes buffered live dispatches
// behind the replayed tasks.
func (m *Manager) EndReplay(exec model.ExecutorID) {
	if r, ok := m.Get(exec); ok {
		r.EndReplay()
	}
}

// Executors returns the registered executors.
func (m *Manager) Executors() []model.ExecutorID {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]model.ExecutorID, 0, len(m.runners))
	for exec := range m.runners {
		out = append(out, exec)
	}
	return out
}

// Count returns the number of registered runners.
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.runners)
}
