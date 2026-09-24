//go:build bench

package taskqueue

import (
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/spec"
)

func BenchmarkTaskQueueEnqueue(b *testing.B) {
	q, mgr, _ := newBenchTaskQueue(b)
	defer q.Close()
	defer mgr.Close()
	ctx := b.Context()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := q.Enqueue(ctx, []byte("benchmark-payload")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTaskQueueDequeueAck(b *testing.B) {
	q, mgr, _ := newBenchTaskQueue(b)
	defer q.Close()
	defer mgr.Close()
	ctx := b.Context()
	for b.Loop() {
		if _, err := q.Enqueue(ctx, []byte("benchmark-payload")); err != nil {
			b.Fatal(err)
		}
		task, err := q.Dequeue(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := q.Ack(ctx, task); err != nil {
			b.Fatal(err)
		}
	}
}

func newBenchTaskQueue(b *testing.B) (*queue, *engine.Manager, engine.Storage) {
	b.Helper()
	dir := b.TempDir()
	mgr, err := engine.OpenManager(dir, engine.Config{
		Durability:    spec.DurabilityAsync,
		MaxSegment:    64 * 1024,
		MaxSegmentAge: time.Hour,
		MaxPayload:    1 << 20,
		MaxCache:      32 << 20,
		MaxFlushBytes: 1 << 20,
		MaxFlushWait:  time.Millisecond,
	}, nil)
	if err != nil {
		b.Fatal(err)
	}
	storage, err := mgr.Create("tasks")
	if err != nil {
		b.Fatal(err)
	}
	queueIface, err := New("tasks", storage, Defaults{})
	if err != nil {
		b.Fatal(err)
	}
	q := queueIface.(*queue)
	return q, mgr, storage
}
