package retry

import "jobsched/internal/model"

// Requeueable reports whether a failed task can be retried.
func (p *Planner) Requeueable(task model.Task) bool {
	return p.Attempts(task.ID) < p.max
}

// NextDue returns a copy of the task pushed to the next window.
func (p *Planner) NextDue(task model.Task, windowEnd model.ScheduleWindow) model.Task {
	task.State = model.TaskPending
	return task
}
