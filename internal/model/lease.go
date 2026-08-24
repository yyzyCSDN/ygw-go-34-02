package model

import "time"

// LeaseState is the lifecycle state of an executor lease.
type LeaseState int

const (
	// LeaseIdle means the lease slot is free.
	LeaseIdle LeaseState = iota
	// LeaseLeased means an executor holds the lease.
	LeaseLeased
	// LeaseRenewing means the lease heartbeat is refreshing.
	LeaseRenewing
	// LeaseEvicted means the lease was reclaimed.
	LeaseEvicted
)

// String renders the lease state.
func (s LeaseState) String() string {
	switch s {
	case LeaseIdle:
		return "idle"
	case LeaseLeased:
		return "leased"
	case LeaseRenewing:
		return "renewing"
	case LeaseEvicted:
		return "evicted"
	default:
		return "unknown"
	}
}

// Lease binds an executor to a slot for a bounded period.
type Lease struct {
	Executor string
	Slot     int
	State    LeaseState
	Expires  time.Time
}
