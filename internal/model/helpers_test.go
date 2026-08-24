package model

import "time"

// MustTime returns a fixed UTC time for tests.
func MustTime() time.Time {
	return time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
}
