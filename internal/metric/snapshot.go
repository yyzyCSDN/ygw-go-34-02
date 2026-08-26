package metric

// Snapshot is a point-in-time view of all counters.
type Snapshot struct {
	Published  int64 `json:"published"`
	Dispatched int64 `json:"dispatched"`
	Failed     int64 `json:"failed"`
	Evicted    int64 `json:"evicted"`
	Succeeded  int64 `json:"succeeded"`
}

// Snapshot captures the current counter values.
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		Published:  m.published.Value(),
		Dispatched: m.dispatched.Value(),
		Failed:     m.failed.Value(),
		Evicted:    m.evicted.Value(),
		Succeeded:  m.succeeded.Value(),
	}
}
