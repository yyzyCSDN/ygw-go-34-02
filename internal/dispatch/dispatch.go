// Package dispatch hands due tasks to leased executors through their
// ordered runner slots.
package dispatch

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"jobsched/internal/clock"
	"jobsched/internal/exec"
	"jobsched/internal/flow"
	"jobsched/internal/lease"
	"jobsched/internal/metric"
	"jobsched/internal/model"
	"jobsched/internal/queue"
	"jobsched/internal/registry"
	"jobsched/internal/store"
)

// ErrOutOfOrder is returned when a batch is not ordered by due time.
var ErrOutOfOrder = errors.New("dispatch batch is not ordered by due time")

// ErrAlreadyReplaying is returned when a runner replay window is open.
var ErrAlreadyReplaying = errors.New("catch-up replay window is already open")

// DispatchError aggregates dispatch failures across a batch.
type DispatchError struct {
	Failed int
	Cause  error
}

// Error renders the aggregated failure.
func (e *DispatchError) Error() string {
	return "dispatch failed for " + itoa(e.Failed) + " task(s): " + e.Cause.Error()
}

// Unwrap returns the first underlying dispatch error.
func (e *DispatchError) Unwrap() error {
	return e.Cause
}

// Is supports errors.Is for the wrapped cause.
func (e *DispatchError) Is(target error) bool {
	return errors.Is(e.Cause, target)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// TaskSource supplies the committed tasks an executor missed.
type TaskSource interface {
	Since(after int64) ([]model.Task, error)
}

type windowEntry struct {
	version uint64
	tasks   []model.Task
}

// Dispatcher schedules queued tasks onto leased executors.
type Dispatcher struct {
	queue   *queue.Queue
	store   *store.Store
	reg     *registry.Registry
	leases  *lease.Manager
	execs   *exec.Manager
	metrics *metric.Metrics
	flow    *flow.Limiter
	mu      sync.Mutex
	cache   map[string]windowEntry
	hits    uint64
	misses  uint64
}

// New creates a dispatcher.
func New(
	q *queue.Queue,
	s *store.Store,
	reg *registry.Registry,
	leases *lease.Manager,
	execs *exec.Manager,
	metrics *metric.Metrics,
	limiter *flow.Limiter,
) *Dispatcher {
	return &Dispatcher{
		queue:   q,
		store:   s,
		reg:     reg,
		leases:  leases,
		execs:   execs,
		metrics: metrics,
		flow:    limiter,
		cache:   make(map[string]windowEntry),
	}
}

// Tick collects the due tasks of one scheduling window and dispatches
// them as a single batch. The queue snapshot is taken once at the start
// of the batch so a task that becomes due mid-batch waits for the next
// window.
func (d *Dispatcher) Tick(ctx context.Context, win clock.Window) error {
	tasks := d.collect(win)
	if len(tasks) == 0 {
		return nil
	}
	return d.Dispatch(ctx, tasks[0].Group, tasks)
}

func (d *Dispatcher) collect(win clock.Window) []model.Task {
	snapshot := d.queue.SnapshotBatch()
	var out []model.Task
	for _, task := range snapshot {
		if win.Contains(task.DueAt) && d.queue.Remove(task.ID) {
			out = append(out, task)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DueAt.Before(out[j].DueAt) })
	return out
}

// Dispatch delivers a batch of tasks to the leased executors. The
// registry view is snapshotted once per batch so a deleted task never
// keeps dispatching from a stale view.
func (d *Dispatcher) Dispatch(ctx context.Context, group string, tasks []model.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	ordered := d.ordered(tasks)
	if ordered == nil {
		return ErrOutOfOrder
	}
	executors := d.leases.Executors()
	if len(executors) == 0 {
		return nil
	}
	executor := executors[0]
	return d.dispatchBatch(ctx, executor, ordered)
}

func (d *Dispatcher) dispatchBatch(ctx context.Context, executor model.ExecutorID, tasks []model.Task) error {
	var firstErr error
	var failed int
	var marked []int64
	for _, task := range tasks {
		if task.DueAt.IsZero() {
			return ErrOutOfOrder
		}
		if err := d.dispatchOne(ctx, executor, task); err != nil {
			failed++
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		marked = append(marked, task.Sequence)
	}
	if len(marked) > 0 {
		d.store.MarkDispatchedBatch(marked, true)
	}
	if firstErr != nil {
		return &DispatchError{Failed: failed, Cause: firstErr}
	}
	return nil
}

func (d *Dispatcher) ordered(tasks []model.Task) []model.Task {
	version := d.reg.Version(tasks[0].Group)
	d.mu.Lock()
	defer d.mu.Unlock()
	if entry, ok := d.cache[tasks[0].Group]; ok {
		if entry.version == version {
			d.hits++
			return entry.tasks
		}
		d.dropGroup(tasks[0].Group)
	}
	d.misses++
	live := make([]model.Task, 0, len(d.reg.GroupTasks(tasks[0].Group)))
	var previous time.Time
	for _, id := range d.reg.GroupTasks(tasks[0].Group) {
		current, ok := d.reg.Get(id)
		if !ok || !d.reg.Alive(id) {
			continue
		}
		if !previous.IsZero() && current.DueAt.Before(previous) {
			d.cache[tasks[0].Group] = windowEntry{version: version, tasks: nil}
			return nil
		}
		previous = current.DueAt
		live = append(live, current)
	}
	d.cache[tasks[0].Group] = windowEntry{version: version, tasks: live}
	return live
}

// dropGroup removes one cached group snapshot.
func (d *Dispatcher) dropGroup(group string) {
	delete(d.cache, group)
}

func (d *Dispatcher) dispatchOne(ctx context.Context, executor model.ExecutorID, task model.Task) error {
	if !d.flow.Allow(executor) {
		return flow.ErrLimited
	}
	inst := model.NewInstance(task, 1)
	inst.DedupeKey = task.DedupeKey
	if err := d.execs.Dispatch(executor, inst); err != nil {
		d.metrics.Failed()
		return err
	}
	d.markDispatched(task.Sequence)
	if err := d.store.Commit(task.Sequence); err != nil {
		d.metrics.Failed()
		return err
	}
	d.reg.SetState(task.ID, model.TaskDispatched)
	d.metrics.Dispatched()
	return nil
}

func (d *Dispatcher) markDispatched(seq int64) {
	d.store.MarkDispatched(seq, true)
}

// CatchUp replays tasks missed by an executor ahead of live dispatches.
func (d *Dispatcher) CatchUp(ctx context.Context, executor model.ExecutorID, from int64) error {
	return d.CatchUpFrom(ctx, executor, from, d.store)
}

// CatchUpFrom opens the runner replay window, reads missed tasks from
// the given source and replays them ahead of live dispatches.
func (d *Dispatcher) CatchUpFrom(ctx context.Context, executor model.ExecutorID, from int64, source TaskSource) error {
	missed, err := source.Since(from)
	if err != nil {
		return err
	}
	for _, task := range missed {
		inst := model.NewInstance(task, 1)
		inst.DedupeKey = task.DedupeKey
		if err := d.execs.Dispatch(executor, inst); err != nil {
			return err
		}
	}
	return nil
}

// CacheStats reports the registry snapshot cache behavior.
func (d *Dispatcher) CacheStats() (hits, misses uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.hits, d.misses
}
