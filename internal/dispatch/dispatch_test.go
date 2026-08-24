package dispatch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"jobsched/internal/exec"
	"jobsched/internal/flow"
	"jobsched/internal/model"
)

// staticSource is a TaskSource that returns a fixed slice of missed tasks.
type staticSource struct {
	tasks []model.Task
}

func (s staticSource) Since(int64) ([]model.Task, error) {
	return s.tasks, nil
}

// gatingSource blocks its Since call until released, so the test can race a
// live dispatch against an in-flight catch-up deterministically.
type gatingSource struct {
	missed  []model.Task
	started chan struct{}
	release chan struct{}
}

func (s *gatingSource) Since(int64) ([]model.Task, error) {
	close(s.started)
	<-s.release
	return s.missed, nil
}

// recordingHandler records the order in which instances reach an executor.
type recordingHandler struct {
	mu      sync.Mutex
	records []string
}

func (h *recordingHandler) handle(inst model.Instance) error {
	h.mu.Lock()
	h.records = append(h.records, inst.TaskID)
	h.mu.Unlock()
	return nil
}

func (h *recordingHandler) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.records))
	copy(out, h.records)
	return out
}

func (h *recordingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.records)
}

// taskWith builds a task with the given id for ordering tests.
func taskWith(id string, due time.Time) model.Task {
	t := model.NewTask("g", []byte(id), due)
	t.ID = id
	t.DedupeKey = id
	return t
}

func mustTimeDispatch() time.Time {
	return time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
}

// newReplayDispatcher builds a minimal Dispatcher with a single registered
// executor whose handler records arrival order.
func newReplayDispatcher(executor model.ExecutorID) (*Dispatcher, *recordingHandler, *exec.Manager) {
	rec := &recordingHandler{}
	mgr := exec.NewManager(rec.handle, 16, time.Second)
	mgr.Add(executor)
	d := &Dispatcher{
		execs: mgr,
		flow:  flow.NewLimiter(flow.Policy{Rate: 1e6, Burst: 1 << 20}),
	}
	return d, rec, mgr
}

// TestCatchUpReplaysBeforeLiveDispatch proves that catch-up (old) tasks
// arrive at the executor ahead of a live task dispatched concurrently
// during the catch-up window, so dependency order is never inverted
// after a reconnect.
//
// Before the fix, CatchUpFrom dispatched missed tasks through the same
// unbuffered Enqueue path the live tick loop uses, with no replay window,
// so a live task dispatched while catch-up was still reading its source
// could land in the runner queue ahead of the replayed (old) tasks.
func TestCatchUpReplaysBeforeLiveDispatch(t *testing.T) {
	const executor = model.ExecutorID("exec-1")
	d, rec, _ := newReplayDispatcher(executor)

	oldTasks := []model.Task{
		taskWith("old-1", mustTimeDispatch()),
		taskWith("old-2", mustTimeDispatch()),
	}
	liveTask := taskWith("new-1", mustTimeDispatch())

	src := &gatingSource{
		missed:  oldTasks,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	// Run catch-up; it will block inside the source.
	catchErr := make(chan error, 1)
	go func() {
		catchErr <- d.CatchUpFrom(context.Background(), executor, 0, src)
	}()

	// Wait until catch-up is guaranteed to be in flight, then dispatch a
	// live task concurrently.
	<-src.started
	liveInst := model.NewInstance(liveTask, 1)
	liveInst.DedupeKey = liveTask.DedupeKey
	if err := d.execs.Dispatch(executor, liveInst); err != nil {
		t.Fatalf("live dispatch: %v", err)
	}

	// Give the live dispatch a chance to race ahead of catch-up.
	time.Sleep(20 * time.Millisecond)

	// Release catch-up so its old tasks are finally delivered.
	close(src.release)
	if err := <-catchErr; err != nil {
		t.Fatalf("CatchUpFrom: %v", err)
	}

	// Drain everything the runner received.
	deadline := time.Now().Add(2 * time.Second)
	for rec.count() < 3 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}

	got := rec.snapshot()
	if len(got) != 3 {
		t.Fatalf("executed %d tasks, want 3 (%v)", len(got), got)
	}

	// The old tasks must precede the live task.
	newIdx := index(got, "new-1")
	oldIdx := maxIndex(got, "old-1", "old-2")
	if newIdx < 0 || oldIdx < 0 {
		t.Fatalf("missing expected tasks in %v", got)
	}
	if oldIdx > newIdx {
		t.Fatalf("dependency order inverted: old tasks arrived after live task in %v", got)
	}
}

// TestCatchUpConcurrentReplayWindowReentrant ensures a second concurrent
// catch-up is rejected instead of interleaving two replay streams. The
// gating source blocks the first catch-up inside its window so the second
// is guaranteed to land while the window is open.
func TestCatchUpConcurrentReplayWindowReentrant(t *testing.T) {
	const executor = model.ExecutorID("exec-1")
	d, _, _ := newReplayDispatcher(executor)

	oldTasks := []model.Task{taskWith("old-1", mustTimeDispatch())}

	src := &gatingSource{
		missed:  oldTasks,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	catchErr := make(chan error, 1)
	go func() {
		catchErr <- d.CatchUpFrom(context.Background(), executor, 0, src)
	}()
	<-src.started

	// Second catch-up must be rejected while the first holds the window.
	err := d.CatchUpFrom(context.Background(), executor, 0, staticSource{tasks: oldTasks})
	if !errors.Is(err, ErrAlreadyReplaying) {
		t.Fatalf("second overlapping catch-up = %v, want ErrAlreadyReplaying", err)
	}

	close(src.release)
	if err := <-catchErr; err != nil {
		t.Fatalf("first catch-up: %v", err)
	}
}

func index(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return -1
}

func maxIndex(s []string, vals ...string) int {
	max := -1
	for _, v := range vals {
		if i := index(s, v); i > max {
			max = i
		}
	}
	return max
}

var _ = errors.New
