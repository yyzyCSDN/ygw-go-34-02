// Package clock supplies the time source used by scheduling windows.
package clock

import "time"

// Clock abstracts the current time so windows are testable.
type Clock interface {
	Now() time.Time
}

// System is the production clock.
type System struct{}

// Now returns the current UTC time.
func (System) Now() time.Time {
	return time.Now().UTC()
}
