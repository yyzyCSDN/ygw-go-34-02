// Package flow implements dispatch rate control so a slow executor
// cannot stall the whole scheduler.
package flow

import "errors"

// ErrLimited is returned when a dispatch burst exceeds the budget.
var ErrLimited = errors.New("dispatch rate limit exceeded")

// DefaultPolicy returns the default dispatch budget.
func DefaultPolicy() Policy {
	return Policy{Rate: 200, Burst: 400}
}
