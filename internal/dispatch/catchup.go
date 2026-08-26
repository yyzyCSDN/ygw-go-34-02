package dispatch

import (
	"jobsched/internal/exec"
)

// ReplayWindow opens the runner replay window, runs the given work and
// closes the window, flushing live dispatches that arrived during the
// replay behind the replayed tasks. Live dispatches are buffered for
// the whole duration of the work so replayed tasks always precede them.
func ReplayWindow(runner *exec.Runner, work func() error) error {
	if !runner.BeginReplay() {
		return ErrAlreadyReplaying
	}
	defer runner.EndReplay()
	return work()
}
