//go:build bench

package taskqueue

import (
	"context"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
)

func BenchmarkTaskQueueEnqueue(b *testing.B) {
	q, mgr, _ := newBenchTaskQueue(b)
	defer q.Close()
	defer mgr.Close()
	ctx := context.Background()
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
	ctx := context.Background()
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

func newBenchTaskQueue(b *testing.B) (*TaskQueue, *engine.Manager, engine.Storage) {
	b.Helper()
	dir := b.TempDir()
	mgr, err := engine.OpenManager(dir, engine.Config{BatchSize: 32, MaxInterval: time.Millisecond, MaxSegmentBytes: 64 * 1024, MaxMessageBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	storage, err := mgr.CreateQueue("tasks")
	if err != nil {
		b.Fatal(err)
	}
	q, err := NewTaskQueue("tasks", storage)
	if err != nil {
		b.Fatal(err)
	}
	return q, mgr, storage
}
