package deps_test

import (
	"fmt"
	"sync"
	"testing"

	"jobsched/internal/deps"
)

func TestDependencyDeleteClearsRefCount(t *testing.T) {
	g := deps.New()
	g.Add("b", []string{"a"})
	if g.Ready("b") {
		t.Fatal("b should wait for dependency a")
	}
	g.Delete("a")
	if !g.Ready("b") {
		t.Fatal("b should become ready after dependency a is deleted")
	}
	if g.Refs("a") != 0 {
		t.Fatalf("deleted dependency still holds %d references", g.Refs("a"))
	}

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			taskID := fmt.Sprintf("c%d", n)
			for j := 0; j < 15; j++ {
				g.Add(taskID, []string{"a"})
				g.Delete("a")
			}
		}(i)
	}
	wg.Wait()
	if got := g.Refs("a"); got > 2 {
		t.Fatalf("reference count leaked to %d after concurrent churn", got)
	}
}
