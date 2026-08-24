package dispatch

import (
	"testing"
	"time"

	"jobsched/internal/model"
	"jobsched/internal/queue"
	"jobsched/internal/registry"
	"jobsched/internal/store"
)

func mustDue() time.Time {
	return time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
}

// registerTask appends, registers and enqueues a task so it is visible to
// both the store and the dispatcher's registry view.
func registerTask(t *testing.T, reg *registry.Registry, q *queue.Queue, st *store.Store, group, id string, due time.Time) model.Task {
	t.Helper()
	task := model.NewTask(group, []byte("x"), due)
	task.ID = id
	seq, err := st.Append(task)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	task.Sequence = seq
	reg.Register(task)
	if err := q.Enqueue(task); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return task
}

// newTestDispatcher wires a dispatcher with a real registry; only the
// ordered-cache path is exercised, so the executor/store wiring is nil.
func newTestDispatcher(reg *registry.Registry) *Dispatcher {
	return New(nil, nil, reg, nil, nil, nil, nil)
}

func ids(tasks []model.Task) []string {
	out := make([]string, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, t.ID)
	}
	return out
}

// TestOrderedCacheInvalidatesOnDelete reproduces the report: after a task
// is taken offline, the per-group snapshot cache must not keep serving
// its id. Before the fix the cache was returned on every hit regardless
// of version, so the removed task kept being handed to executors.
func TestOrderedCacheInvalidatesOnDelete(t *testing.T) {
	reg := registry.New()
	d := newTestDispatcher(reg)

	const group = "batch/etl"
	live := model.NewTask(group, []byte("x"), mustDue())
	live.ID = "live-task"
	offline := model.NewTask(group, []byte("x"), mustDue())
	offline.ID = "offline-task"
	reg.Register(live)
	reg.Register(offline)

	batch := []model.Task{live, offline}

	// First call primes the cache with both tasks.
	got := d.ordered(batch)
	if len(got) != 2 {
		t.Fatalf("initial ordered = %v, want both tasks", ids(got))
	}

	// Take the second task offline. The group version must move so the
	// cached snapshot is no longer trusted.
	reg.Delete(offline.ID)

	// The caller still hands the stale list (e.g. a retry round reusing
	// the old batch). ordered must consult the live registry and drop
	// the removed id instead of replaying the cached copy.
	got = d.ordered(batch)
	if contains(ids(got), offline.ID) {
		t.Fatalf("offline task served from cache after delete; got %v", ids(got))
	}
	if !contains(ids(got), live.ID) {
		t.Fatalf("live task dropped after delete; got %v", ids(got))
	}
}

// TestOrderedCacheInvalidatesOnStateChange guards the companion path: a
// state transition bumps the version too, so the cache must be rebuilt
// rather than served as-is.
func TestOrderedCacheInvalidatesOnStateChange(t *testing.T) {
	reg := registry.New()
	d := newTestDispatcher(reg)

	const group = "batch/etl"
	task := model.NewTask(group, []byte("x"), mustDue())
	task.ID = "state-task"
	reg.Register(task)

	batch := []model.Task{task}

	if got := d.ordered(batch); len(got) != 1 {
		t.Fatalf("initial ordered = %v, want the task", ids(got))
	}

	// Mark the task removed. A rebuilt snapshot must observe the removed
	// state and drop the task; a stale cache would still return it.
	reg.SetState(task.ID, model.TaskRemoved)

	if got := d.ordered(batch); len(got) != 0 {
		t.Fatalf("removed task served from cache; got %v", ids(got))
	}
}

// TestOrderedCacheServesStableGroup confirms the cache is still used when
// nothing changed, so the fix does not regress the hit path.
func TestOrderedCacheServesStableGroup(t *testing.T) {
	reg := registry.New()
	d := newTestDispatcher(reg)

	const group = "batch/etl"
	task := model.NewTask(group, []byte("x"), mustDue())
	task.ID = "stable-task"
	reg.Register(task)

	batch := []model.Task{task}

	first := d.ordered(batch)
	second := d.ordered(batch)

	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("ordered = %v then %v, want the task both times", ids(first), ids(second))
	}
	hits, misses := d.CacheStats()
	if hits != 1 || misses != 1 {
		t.Fatalf("cache stats = hit %d miss %d, want 1/1", hits, misses)
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
