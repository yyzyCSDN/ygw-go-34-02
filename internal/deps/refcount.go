package deps

// refcount.go holds the dependency bookkeeping helpers used by the
// console and the calibration surface.

// EdgeCount returns the total number of pending dependency edges.
func (g *Graph) EdgeCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	total := 0
	for _, n := range g.nodes {
		total += n.pending
	}
	return total
}

// clearRefs drops every reference that points at taskID and returns how
// many dependent edges were released. It is the single cleanup point
// used by Delete so reference counts never leak.
func (g *Graph) clearRefs(taskID string) int {
	cleared := 0
	delete(g.refs, taskID)
	for _, n := range g.nodes {
		if _, ok := n.dependents[taskID]; ok {
			delete(n.dependents, taskID)
			cleared++
			if n.pending > 0 {
				n.pending--
			}
		}
	}
	return cleared
}
