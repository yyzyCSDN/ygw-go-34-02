package deps

import "testing"

func TestDependencyReadyAndSatisfy(t *testing.T) {
	g := New()
	g.Add("b", []string{"a"})
	if g.Ready("b") {
		t.Fatal("b should not be ready while a is pending")
	}
	if len(g.Deps("b")) != 1 {
		t.Fatalf("deps = %v, want [a]", g.Deps("b"))
	}
	g.Satisfy("a")
	if !g.Ready("b") {
		t.Fatal("b should be ready after a is satisfied")
	}
	if !g.Ready("c") {
		t.Fatal("unknown task should be ready")
	}
	if g.EdgeCount() != 0 {
		t.Fatalf("edges = %d, want 0", g.EdgeCount())
	}
}

func TestDependencyRefs(t *testing.T) {
	g := New()
	g.Add("b", []string{"a"})
	g.Add("c", []string{"a"})
	if g.Refs("a") != 2 {
		t.Fatalf("refs(a) = %d, want 2", g.Refs("a"))
	}
	if g.NodeCount() != 2 {
		t.Fatalf("nodes = %d, want 2", g.NodeCount())
	}
}
