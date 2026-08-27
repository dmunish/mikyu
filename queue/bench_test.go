package queue_test

import (
	"context"
	"testing"

	"github.com/dmunish/mikyu/queue"
)

func benchQueue(b *testing.B) *queue.Queue {
	b.Helper()
	m, err := queue.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	q, err := m.CreateQueue("bench")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { m.Close() })
	return q
}

func BenchmarkProduceSerial(b *testing.B) {
	ctx := context.Background()
	q := benchQueue(b)
	payload := make([]byte, 100)

	for b.Loop() {
		if _, err := q.Produce(ctx, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProduceParallel(b *testing.B) {
	ctx := context.Background()
	q := benchQueue(b)
	payload := make([]byte, 500)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := q.Produce(ctx, payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}
