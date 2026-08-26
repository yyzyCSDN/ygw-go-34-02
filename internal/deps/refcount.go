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
