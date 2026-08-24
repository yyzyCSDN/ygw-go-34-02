package heartbeat

import (
	"log"
	"time"

	"jobsched/internal/exec"
	"jobsched/internal/lease"
	"jobsched/internal/metric"
	"jobsched/internal/model"
)

// Evictor reclaims a stale executor lease and its runner slot.
type Evictor struct {
	leases  *lease.Manager
	execs   *exec.Manager
	metrics *metric.Metrics
}

// NewEvictor creates an eviction coordinator.
func NewEvictor(leases *lease.Manager, execs *exec.Manager, metrics *metric.Metrics) *Evictor {
	return &Evictor{
		leases:  leases,
		execs:   execs,
		metrics: metrics,
	}
}

// Evict reclaims the lease and waits for the runner's in-flight task to
// finish before releasing the slot.
func (e *Evictor) Evict(exec model.ExecutorID) {
	if !e.leases.Evict(exec) {
		return
	}
	if runner, ok := e.execs.Get(exec); ok {
		log.Printf("evicting executor %s with in-flight work", exec)
		runner.Shutdown()
		if !runner.Wait(3*time.Second) && !runner.IsClosed() {
			log.Printf("runner of %s did not release within timeout", exec)
		}
	}
	e.execs.Remove(exec)
	e.metrics.Evicted()
}
