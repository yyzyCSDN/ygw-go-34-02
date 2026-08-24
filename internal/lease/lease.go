// Package lease manages the executor lease table.
package lease

import (
	"errors"
	"sync"
	"time"

	"jobsched/internal/model"
)

// ErrNoLease is returned when an executor does not hold a lease.
var ErrNoLease = errors.New("executor has no lease")

// Manager owns the leases of registered executors.
type Manager struct {
	mu      sync.Mutex
	leases  map[model.ExecutorID]*model.Lease
	slotOf  map[model.ExecutorID]int
	next    int
	timeout time.Duration
}

// New creates a lease manager with the given lease duration.
func New(timeout time.Duration) *Manager {
	return &Manager{
		leases:  make(map[model.ExecutorID]*model.Lease),
		slotOf:  make(map[model.ExecutorID]int),
		timeout: timeout,
	}
}

// Acquire grants a slot to an executor.
func (m *Manager) Acquire(exec model.ExecutorID) (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slot, ok := m.slotOf[exec]; ok {
		return slot, true
	}
	slot := m.next
	m.next++
	m.leases[exec] = &model.Lease{
		Executor: string(exec),
		Slot:     slot,
		State:    model.LeaseLeased,
		Expires:  time.Now().UTC().Add(m.timeout),
	}
	m.slotOf[exec] = slot
	return slot, true
}

// Renew refreshes the lease expiry. Renewal fails for leases that are
// already evicted or idle so a reclaimed slot can never be revived.
func (m *Manager) Renew(exec model.ExecutorID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.leases[exec]
	if !ok {
		return false
	}
	if l.State == model.LeaseEvicted || l.State == model.LeaseIdle {
		return false
	}
	l.State = model.LeaseLeased
	l.Expires = time.Now().UTC().Add(m.timeout)
	return true
}

// Evict reclaims the lease of an executor.
func (m *Manager) Evict(exec model.ExecutorID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.leases[exec]
	if !ok {
		return false
	}
	l.State = model.LeaseEvicted
	l.Expires = time.Time{}
	delete(m.slotOf, exec)
	return true
}

// Expired reports whether an executor's lease has lapsed.
func (m *Manager) Expired(exec model.ExecutorID, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.leases[exec]
	if !ok {
		return true
	}
	return l.State == model.LeaseEvicted || now.After(l.Expires)
}

// Lease returns the lease of an executor.
func (m *Manager) Lease(exec model.ExecutorID) (*model.Lease, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.leases[exec]
	if !ok {
		return nil, false
	}
	copy := *l
	return &copy, true
}

// Executors returns all leased executors.
func (m *Manager) Executors() []model.ExecutorID {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]model.ExecutorID, 0, len(m.leases))
	for exec := range m.leases {
		out = append(out, exec)
	}
	return out
}

// ExpiredLeases returns the executors whose leases have lapsed.
func (m *Manager) ExpiredLeases(now time.Time) []model.ExecutorID {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []model.ExecutorID
	for exec, l := range m.leases {
		if l.State == model.LeaseEvicted || now.After(l.Expires) {
			out = append(out, exec)
		}
	}
	return out
}

// Count returns the number of leases.
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.leases)
}
