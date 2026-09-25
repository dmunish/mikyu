//go:build bench

package node

import (
	"fmt"
	"testing"

	"github.com/dmunish/mikyu/internal/enginetest"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
)

// sweepQueueCount matches the plan's sweep budget: 10,000 installed task queues.
const sweepQueueCount = 10_000

// benchEntry builds an installed, already-resolved catalog entry.
func benchEntry(b *testing.B, name string, storage *enginetest.MemStorage) *taskEntry {
	b.Helper()
	q, err := taskqueue.New(name, storage, taskqueue.Defaults{}, nil)
	if err != nil {
		b.Fatal(err)
	}
	if err := taskqueue.Checkpoint(q); err != nil {
		b.Fatal(err)
	}
	e := &taskEntry{ready: make(chan struct{}), q: q}
	close(e.ready)
	return e
}

func BenchmarkLookupTaskQueue(b *testing.B) {
	storage := enginetest.NewMemStorage("bench", 1<<20, 64<<20)
	n := &Node{taskCatalog: map[string]*taskEntry{"jobs": benchEntry(b, "jobs", storage)}}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := n.taskEntryFor("jobs"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSharedCheckpointSweep(b *testing.B) {
	storage := enginetest.NewMemStorage("bench", 1<<20, 64<<20)
	n := &Node{taskCatalog: make(map[string]*taskEntry, sweepQueueCount)}
	for i := range sweepQueueCount {
		name := fmt.Sprintf("q%05d", i)
		n.taskCatalog[name] = benchEntry(b, name, storage)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		n.sweepCheckpoints()
	}
}
