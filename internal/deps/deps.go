// Package deps implements the task dependency graph with reference
// counting so deleting a task reclaims every edge that points at it.
package deps

import (
	"sort"
	"sync"
)

type node struct {
	dependents map[string]struct{}
	pending    int
}

// Graph tracks dependency edges between tasks.
type Graph struct {
	mu    sync.Mutex
	nodes map[string]*node
	refs  map[string]int
}

// New creates an empty dependency graph.
func New() *Graph {
	return &Graph{
		nodes: make(map[string]*node),
		refs:  make(map[string]int),
	}
}

// Add registers taskID as depending on the given dependency ids.
func (g *Graph) Add(taskID string, dependsOn []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := g.nodes[taskID]
	if n == nil {
		n = &node{dependents: make(map[string]struct{})}
		g.nodes[taskID] = n
	}
	for _, dep := range dependsOn {
		if dep == "" {
			continue
		}
		if _, ok := n.dependents[dep]; !ok {
			n.dependents[dep] = struct{}{}
			n.pending++
			g.refs[dep]++
		}
	}
}

// Satisfy marks a dependency as satisfied and notifies dependents.
func (g *Graph) Satisfy(depID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, n := range g.nodes {
		if _, ok := n.dependents[depID]; ok {
			delete(n.dependents, depID)
			if n.pending > 0 {
				n.pending--
			}
		}
	}
	delete(g.refs, depID)
}

// Delete removes a task node and every edge that points at it.
func (g *Graph) Delete(taskID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, hadNode := g.nodes[taskID]
	delete(g.nodes, taskID)
	g.clearRefs(taskID)
	return hadNode
}

// Ready reports whether a task has no unsatisfied dependencies.
func (g *Graph) Ready(taskID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := g.nodes[taskID]
	return n == nil || n.pending == 0
}

// Refs returns how many dependents still reference taskID.
func (g *Graph) Refs(taskID string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.refs[taskID]
}

// Deps returns the pending dependencies of a task.
func (g *Graph) Deps(taskID string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := g.nodes[taskID]
	if n == nil {
		return nil
	}
	out := make([]string, 0, len(n.dependents))
	for dep := range n.dependents {
		out = append(out, dep)
	}
	sort.Strings(out)
	return out
}

// NodeCount returns the number of graph nodes.
func (g *Graph) NodeCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.nodes)
}
