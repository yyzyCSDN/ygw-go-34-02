// Package registry tracks the live task table and per-group versions.
package registry

import (
	"sort"
	"sync"

	"jobsched/internal/model"
)

// TaskView is the public view of a task for the console.
type TaskView struct {
	ID        string          `json:"id"`
	Group     string          `json:"group"`
	State     model.TaskState `json:"state"`
	StateText string          `json:"state_text"`
	DueAt     string          `json:"due_at"`
}

// Registry is the task table used by the scheduler.
type Registry struct {
	mu           sync.Mutex
	byID         map[string]model.Task
	versions     map[string]uint64
	incarnations map[string]uint64
	groups       map[string][]string
}

// New creates an empty task registry.
func New() *Registry {
	return &Registry{
		byID:         make(map[string]model.Task),
		versions:     make(map[string]uint64),
		incarnations: make(map[string]uint64),
		groups:       make(map[string][]string),
	}
}

// Register records a task. A task id that already finished or was
// removed starts a fresh incarnation with its own execution identity.
func (r *Registry) Register(task model.Task) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.byID[task.ID]; ok {
		if old.State == model.TaskDone || old.State == model.TaskRemoved {
			r.incarnations[task.ID]++
		}
	} else {
		r.incarnations[task.ID] = 1
	}
	r.byID[task.ID] = task
	r.groups[task.Group] = append(r.groups[task.Group], task.ID)
	r.versions[task.Group]++
}

// Delete removes a task from the table and bumps its group version.
func (r *Registry) Delete(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	task, ok := r.byID[id]
	if !ok {
		return
	}
	delete(r.byID, id)
	if ids := r.groups[task.Group]; ids != nil {
		kept := ids[:0]
		for _, existing := range ids {
			if existing != id {
				kept = append(kept, existing)
			}
		}
		r.groups[task.Group] = kept
	}
	r.versions[task.Group]++
}

// Get returns the task for an id.
func (r *Registry) Get(id string) (model.Task, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	task, ok := r.byID[id]
	return task, ok
}

// SetState updates the state of a task.
func (r *Registry) SetState(id string, state model.TaskState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if task, ok := r.byID[id]; ok {
		task.State = state
		r.byID[id] = task
		r.versions[task.Group]++
	}
}

// Version returns the current version for a group.
func (r *Registry) Version(group string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.versions[group]
}

// Incarnation returns the execution incarnation bound to a task id.
func (r *Registry) Incarnation(id string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.incarnations[id]
}

// Alive reports whether a task id maps to a live, non-removed task.
func (r *Registry) Alive(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	task, ok := r.byID[id]
	return ok && task.State != model.TaskRemoved
}

// GroupTasks returns the registered task ids of a group.
func (r *Registry) GroupTasks(group string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.groups[group]))
	copy(out, r.groups[group])
	return out
}

// List returns all registered tasks.
func (r *Registry) List() []model.Task {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]model.Task, 0, len(r.byID))
	for _, task := range r.byID {
		out = append(out, task)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Snapshot returns the console view of every task.
func (r *Registry) Snapshot() []TaskView {
	tasks := r.List()
	out := make([]TaskView, 0, len(tasks))
	for _, task := range tasks {
		out = append(out, TaskView{
			ID:        task.ID,
			Group:     task.Group,
			State:     task.State,
			StateText: task.State.String(),
			DueAt:     task.DueAt.Format("2006-01-02T15:04:05Z07:00"),
		})
	}
	return out
}
