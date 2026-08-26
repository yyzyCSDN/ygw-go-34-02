package metric

import "testing"

func TestCounters(t *testing.T) {
	m := New()
	m.Published()
	m.Dispatched()
	m.Dispatched()
	m.Failed()
	m.Evicted()
	m.Succeeded()
	snap := m.Snapshot()
	if snap.Published != 1 || snap.Dispatched != 2 || snap.Failed != 1 || snap.Evicted != 1 || snap.Succeeded != 1 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}
