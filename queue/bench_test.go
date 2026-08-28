package queue_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/dmunish/mikyu/queue"
)

// benchQueueFilled opens a queue and pre-fills it with n messages so the
// consume benchmarks exercise only the read path, not the produce path.
func benchQueueFilled(b *testing.B, n int) *queue.Queue {
	b.Helper()
	q := benchQueue(b)
	ctx := context.Background()
	payload := make([]byte, 7*1024)
	for i := 0; i < n; i++ {
		if _, err := q.Produce(ctx, payload); err != nil {
			b.Fatal(err)
		}
	}
	return q
}

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

func BenchmarkConsumeSequential(b *testing.B) {
	ctx := context.Background()
	const n = 10_000
	q := benchQueueFilled(b, n)

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		offset := uint64(i % n)
		if _, err := q.Read(ctx, offset); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConsumeRandom(b *testing.B) {
	ctx := context.Background()
	const n = 10_000
	q := benchQueueFilled(b, n)

	rng := rand.New(rand.NewSource(1))
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		offset := uint64(rng.Intn(n))
		if _, err := q.Read(ctx, offset); err != nil {
			b.Fatal(err)
		}
	}
}
