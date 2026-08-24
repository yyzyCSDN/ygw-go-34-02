// Package metric tracks scheduler counters exposed by the console.
package metric

// Metrics is the scheduler-wide counter set.
type Metrics struct {
	published  *Counters
	dispatched *Counters
	failed     *Counters
	evicted    *Counters
	succeeded  *Counters
}

// New builds an empty counter set.
func New() *Metrics {
	return &Metrics{
		published:  NewCounters(),
		dispatched: NewCounters(),
		failed:     NewCounters(),
		evicted:    NewCounters(),
		succeeded:  NewCounters(),
	}
}

// Published records one accepted publish.
func (m *Metrics) Published() { m.published.Inc() }

// Dispatched records one handed-off task.
func (m *Metrics) Dispatched() { m.dispatched.Inc() }

// Failed records one dispatch or execution failure.
func (m *Metrics) Failed() { m.failed.Inc() }

// Evicted records one evicted executor.
func (m *Metrics) Evicted() { m.evicted.Inc() }

// Succeeded records one completed task.
func (m *Metrics) Succeeded() { m.succeeded.Inc() }
