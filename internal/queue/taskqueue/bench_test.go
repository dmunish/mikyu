//go:build bench

package taskqueue

import (
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/spec"
)

func BenchmarkTaskQueueEnqueue(b *testing.B) {
	q, mgr, _ := newBenchTaskQueue(b, false)
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
	q, mgr, _ := newBenchTaskQueue(b, false)
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

func BenchmarkTaskQueueDequeueWithMetrics(b *testing.B)    { benchmarkTaskQueueDequeue(b, true) }
func BenchmarkTaskQueueDequeueWithoutMetrics(b *testing.B) { benchmarkTaskQueueDequeue(b, false) }

func benchmarkTaskQueueDequeue(b *testing.B, withMetrics bool) {
	q, mgr, _ := newBenchTaskQueue(b, withMetrics)
	defer q.Close()
	defer mgr.Close()
	ctx := b.Context()
	for b.Loop() {
		if _, err := q.Enqueue(ctx, []byte("benchmark-payload")); err != nil {
			b.Fatal(err)
		}
		if _, err := q.Dequeue(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// newBenchTaskQueue opens a bench queue, optionally instrumented.
func newBenchTaskQueue(b *testing.B, withMetrics bool) (*queue, *engine.Manager, engine.Storage) {
	b.Helper()
	dir := b.TempDir()
	cfg := engine.Config{
		Durability:    spec.DurabilityAsync,
		MaxSegment:    64 * 1024,
		MaxSegmentAge: time.Hour,
		MaxPayload:    1 << 20,
		MaxCache:      32 << 20,
		MaxFlushBytes: 1 << 20,
		MaxFlushWait:  time.Millisecond,
	}
	var (
		mgr    *engine.Manager
		err    error
		scopes []*metrics.Scope
	)
	if withMetrics {
		reg := metrics.NewRegistry(metrics.Config{})
		mgr, err = engine.OpenManager(dir, cfg, nil, reg)
		scopes = append(scopes, reg.Scope("tasks"))
	} else {
		mgr, err = engine.OpenManager(dir, cfg, nil)
	}
	if err != nil {
		b.Fatal(err)
	}
	storage, err := mgr.Create("tasks")
	if err != nil {
		b.Fatal(err)
	}
	queueIface, err := New("tasks", storage, Defaults{}, nil, scopes...)
	if err != nil {
		b.Fatal(err)
	}
	q := queueIface.(*queue)
	return q, mgr, storage
}
