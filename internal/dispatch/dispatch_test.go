package dispatch

import (
	"context"
	"sync"
	"testing"
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

func mustTime() time.Time {
	return time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
}

// dispatchLog is a synchronous record of every dispatchOne invocation,
// kept in dispatch order so a repeated task id surfaces as a duplicate.
type dispatchLog struct {
	mu   sync.Mutex
	seen []string
}

func (l *dispatchLog) record(taskID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen = append(l.seen, taskID)
}

func (l *dispatchLog) ids() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.seen))
	copy(out, l.seen)
	return out
}

func (l *dispatchLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.seen)
}

func (l *dispatchLog) duplicates() []string {
	ids := l.ids()
	var dups []string
	counts := make(map[string]int)
	for _, id := range ids {
		counts[id]++
	}
	for id, n := range counts {
		if n > 1 {
			dups = append(dups, id)
		}
	}
	return dups
}

// newDispatcher builds a dispatcher wired to a runner with a no-op
// handler; dispatches are recorded synchronously through the
// onDispatched hook so tests observe exact batch membership without
// depending on the runner's asynchronous execution. The returned
// shutdown closes the runner so no goroutine leaks past the test.
func newDispatcher(t *testing.T, q *queue.Queue, st *store.Store, reg *registry.Registry, log *dispatchLog) (*Dispatcher, func()) {
	t.Helper()
	leases := lease.New(time.Hour)
	execs := exec.NewManager(func(inst model.Instance) error { return nil }, 8, time.Second)
	limiter := flow.NewLimiter(flow.Policy{Rate: 1000, Burst: 1000})
	d := New(q, st, reg, leases, execs, metric.New(), limiter)
	d.SetOnDispatched(func(task model.Task) { log.record(task.ID) })
	executor := model.ExecutorID("worker-1")
	execs.Add(executor)
	leases.Acquire(executor)
	runner, _ := execs.Get(executor)
	shutdown := func() {
		if runner != nil {
			runner.Shutdown()
		}
	}
	return d, shutdown
}

func appendAndQueue(t *testing.T, st *store.Store, reg *registry.Registry, q *queue.Queue, task model.Task) {
	t.Helper()
	seq, err := st.Append(task)
	if err != nil {
		t.Fatal(err)
	}
	task.Sequence = seq
	reg.Register(task)
	if err := q.Enqueue(task); err != nil {
		t.Fatal(err)
	}
}

// TestTickSnapshotConsistentMidBatchEnqueue reproduces the reported
// race: while a tick is dispatching the due batch, a task that just
// became due is enqueued concurrently. The snapshot-consistent window
// must keep that latecomer out of the in-flight batch and hand it to
// the next window instead of folding it into the current (half) batch,
// which previously let the same task id show up twice in the dispatch
// log.
func TestTickSnapshotConsistentMidBatchEnqueue(t *testing.T) {
	st := store.NewStore(32)
	q := queue.New(32)
	reg := registry.New()
	base := mustTime()
	win := clock.Window{Begin: base, End: base.Add(time.Second)}

	// Two tasks due inside the window: they form the batch.
	t1 := model.NewTask("g", []byte("1"), base)
	t2 := model.NewTask("g", []byte("2"), base.Add(100*time.Millisecond))
	appendAndQueue(t, st, reg, q, t1)
	appendAndQueue(t, st, reg, q, t2)

	// The latecomer is due inside the same window but is published only
	// once the batch is already in flight. It must wait for the next
	// tick. Under the old live-pop loop it was popped in the same Tick.
	late := model.NewTask("g", []byte("late"), base.Add(200*time.Millisecond))
	log := &dispatchLog{}
	d, shutdown := newDispatcher(t, q, st, reg, log)
	defer shutdown()
	d.SetOnDispatched(func(task model.Task) {
		log.record(task.ID)
		if task.ID == t1.ID {
			// Simulate a concurrent publish landing mid-batch, exactly
			// when the scheduler is handing the batch off.
			appendAndQueue(t, st, reg, q, late)
		}
	})

	if err := d.Tick(context.Background(), win); err != nil {
		t.Fatalf("tick: %v", err)
	}

	// The first window must contain only the original two tasks: the
	// latecomer stayed out of the snapshot taken at the start of the
	// batch.
	if got := log.count(); got != 2 {
		t.Fatalf("dispatched %d task(s) in window 1, want 2 (latecomer leaked into batch)", got)
	}
	if dups := log.duplicates(); len(dups) != 0 {
		t.Fatalf("duplicate dispatch ids in window 1: %v", dups)
	}

	// The latecomer is still queued and must be dispatched by the next
	// tick on its own batch, exactly once.
	if q.Len() != 1 {
		t.Fatalf("queue len after window 1 = %d, want 1 (latecomer)", q.Len())
	}
	if err := d.Tick(context.Background(), win); err != nil {
		t.Fatalf("tick 2: %v", err)
	}
	if got := log.count(); got != 3 {
		t.Fatalf("dispatched %d task(s) after window 2, want 3", got)
	}
	if dups := log.duplicates(); len(dups) != 0 {
		t.Fatalf("duplicate dispatch ids after window 2: %v", dups)
	}
}

// TestTickRequeuesOutOfWindow confirms a task beyond the window is left
// for a later tick rather than dropped or dispatched early.
func TestTickRequeuesOutOfWindow(t *testing.T) {
	st := store.NewStore(32)
	q := queue.New(32)
	reg := registry.New()
	base := mustTime()
	win := clock.Window{Begin: base, End: base.Add(time.Second)}

	due := model.NewTask("g", []byte("due"), base)
	later := model.NewTask("g", []byte("later"), base.Add(2*time.Second))
	appendAndQueue(t, st, reg, q, due)
	appendAndQueue(t, st, reg, q, later)

	log := &dispatchLog{}
	d, shutdown := newDispatcher(t, q, st, reg, log)
	defer shutdown()

	if err := d.Tick(context.Background(), win); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got := log.count(); got != 1 {
		t.Fatalf("dispatched %d, want only the in-window task", got)
	}
	if q.Len() != 1 {
		t.Fatalf("queue len = %d, want the out-of-window task kept", q.Len())
	}
}

// TestTickNoExecutorsKeepsBatch ensures the batch is put back when no
// executor holds a lease, so tasks are not lost.
func TestTickNoExecutorsKeepsBatch(t *testing.T) {
	st := store.NewStore(32)
	q := queue.New(32)
	reg := registry.New()
	base := mustTime()
	win := clock.Window{Begin: base, End: base.Add(time.Second)}

	task := model.NewTask("g", []byte("x"), base)
	appendAndQueue(t, st, reg, q, task)

	leases := lease.New(time.Hour)
	execs := exec.NewManager(func(inst model.Instance) error { return nil }, 8, time.Second)
	d := New(q, st, reg, leases, execs, metric.New(), flow.NewLimiter(flow.Policy{Rate: 1000, Burst: 1000}))
	if err := d.Tick(context.Background(), win); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if q.Len() != 1 {
		t.Fatalf("queue len = %d, want the task kept when no executor is leased", q.Len())
	}
}
