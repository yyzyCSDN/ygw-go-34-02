// Package exec runs task instances on executor slots with ordering and
// per-instance dedupe.
package exec

import (
	"errors"
	"sync"
	"time"

	"jobsched/internal/model"
)

var (
	// ErrClosed is returned when a slot targets a released runner.
	ErrClosed = errors.New("executor slot is closed")
	// ErrDuplicate is returned when an instance with the same dedupe key
	// is already in flight.
	ErrDuplicate = errors.New("task instance already in flight")
	// ErrFull is returned when the runner queue stays full.
	ErrFull = errors.New("executor queue is full")
)

// Handler executes one instance.
type Handler func(model.Instance) error

// Runner delivers instances to one executor slot in order.
type Runner struct {
	mu         sync.Mutex
	in         chan model.Instance
	inflight   map[string]struct{}
	replay     bool
	buffer     []model.Instance
	closed     bool
	handler    Handler
	timeout    time.Duration
	inflightWG sync.WaitGroup
	done       chan struct{}
	stop       chan struct{}
}

// NewRunner creates a runner for an executor.
func NewRunner(handler Handler, queueSize int, timeout time.Duration) *Runner {
	r := &Runner{
		in:       make(chan model.Instance, queueSize),
		inflight: make(map[string]struct{}),
		handler:  handler,
		timeout:  timeout,
		done:     make(chan struct{}),
		stop:     make(chan struct{}),
	}
	go r.run()
	return r
}

// Enqueue queues an instance for execution, buffering it while a replay
// window is open.
func (r *Runner) Enqueue(inst model.Instance) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrClosed
	}
	if _, ok := r.inflight[inst.DedupeKey]; ok {
		r.mu.Unlock()
		return ErrDuplicate
	}
	if r.replay {
		r.buffer = append(r.buffer, inst)
		r.mu.Unlock()
		return nil
	}
	// Fast path: reserve the dedupe slot and send without blocking.
	r.inflight[inst.DedupeKey] = struct{}{}
	select {
	case r.in <- inst:
		r.mu.Unlock()
		return nil
	default:
	}
	delete(r.inflight, inst.DedupeKey)
	r.mu.Unlock()
	// Slow path: wait for queue space without holding the runner lock so
	// the receiver can still finish its current instance.
	select {
	case r.in <- inst:
		r.mu.Lock()
		r.inflight[inst.DedupeKey] = struct{}{}
		r.mu.Unlock()
		return nil
	case <-time.After(r.timeout):
		return ErrFull
	case <-r.stop:
		return ErrClosed
	}
}

// ReplayWrite queues a replayed instance directly, bypassing the replay
// buffer.
func (r *Runner) ReplayWrite(inst model.Instance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	return r.sendLocked(inst)
}

// BeginReplay opens the replay window for the runner.
func (r *Runner) BeginReplay() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.replay {
		return false
	}
	r.replay = true
	r.buffer = nil
	return true
}

// EndReplay closes the replay window and flushes buffered live
// instances behind the replayed ones.
func (r *Runner) EndReplay() {
	r.mu.Lock()
	flush := r.buffer
	r.replay = false
	r.buffer = nil
	r.mu.Unlock()
	for _, inst := range flush {
		r.mu.Lock()
		_ = r.sendLocked(inst)
		r.mu.Unlock()
	}
}

// InReplay reports whether the replay window is open.
func (r *Runner) InReplay() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.replay
}

// Shutdown stops the runner: the in-flight instance finishes first,
// then the queue is closed and the slot is released.
func (r *Runner) Shutdown() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.mu.Unlock()
	r.inflightWG.Wait()
	close(r.stop)
	close(r.done)
}

// Wait blocks until the runner has released its slot, or until the
// timeout elapses.
func (r *Runner) Wait(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-r.done:
		return true
	case <-timer.C:
		return false
	}
}

// IsClosed reports whether the runner was shut down.
func (r *Runner) IsClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

// QueueDepth returns the number of instances waiting in the queue.
func (r *Runner) QueueDepth() int {
	return len(r.in)
}

func (r *Runner) sendLocked(inst model.Instance) error {
	r.inflight[inst.DedupeKey] = struct{}{}
	select {
	case r.in <- inst:
		return nil
	case <-time.After(r.timeout):
		delete(r.inflight, inst.DedupeKey)
		return ErrFull
	}
}

func (r *Runner) run() {
	for {
		select {
		case inst := <-r.in:
			r.mu.Lock()
			if r.closed {
				r.mu.Unlock()
				return
			}
			r.inflightWG.Add(1)
			r.mu.Unlock()
			r.execute(inst)
		case <-r.stop:
			return
		}
	}
}

func (r *Runner) execute(inst model.Instance) {
	defer r.inflightWG.Done()
	_ = r.handler(inst)
	r.mu.Lock()
	delete(r.inflight, inst.DedupeKey)
	r.mu.Unlock()
}
