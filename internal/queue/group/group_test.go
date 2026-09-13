package group

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestBaseMarkCommitClose(t *testing.T) {
	var (
		mu        sync.Mutex
		commits   []uint64
		callCount atomic.Int32
	)

	g := &Base{
		Name: "orders",
		CommitFn: func(_ context.Context, offset uint64) error {
			callCount.Add(1)
			mu.Lock()
			commits = append(commits, offset)
			mu.Unlock()
			return nil
		},
	}

	g.Mark(5)
	g.Mark(2)
	g.Mark(9)
	if g.HighWater != 10 {
		t.Fatalf("HighWater = %d, want 10", g.HighWater)
	}

	if err := g.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	if got := callCount.Load(); got != 2 {
		t.Fatalf("commit count = %d, want 2", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(commits) != 2 || commits[0] != 10 || commits[1] != 10 {
		t.Fatalf("commits = %v, want [10 10]", commits)
	}
}
