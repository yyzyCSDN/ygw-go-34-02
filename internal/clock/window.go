package clock

import "time"

// Window describes the next scheduling tick.
type Window struct {
	Begin time.Time
	End   time.Time
}

// NextWindow returns the tick interval containing now.
func NextWindow(now time.Time, interval time.Duration) Window {
	start := now.Truncate(interval)
	return Window{Begin: start, End: start.Add(interval)}
}

// Contains reports whether a task's due time falls inside the window.
func (w Window) Contains(due time.Time) bool {
	return !due.Before(w.Begin) && due.Before(w.End)
}
