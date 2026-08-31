// queue/bench_test.go

package queue_test

import (
	"context"
	"math/rand/v2"
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

func benchQueueFilled(b *testing.B, n int) *queue.Queue {
	b.Helper()
	q := benchQueue(b)
	ctx := context.Background()

	const batchSize = 1000
	payload := make([]byte, 1024)
	batch := make([][]byte, batchSize)
	for i := range batch {
		batch[i] = payload
	}

	// Pre-fill using atomic ProduceBatch (takes milliseconds instead of minutes)
	for i := 0; i < n; i += batchSize {
		if _, err := q.ProduceBatch(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
	return q
}

func BenchmarkProduceSerial(b *testing.B) {
	ctx := context.Background()
	q := benchQueue(b)
	payload := make([]byte, 256)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := q.Produce(ctx, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProduceParallel(b *testing.B) {
	ctx := context.Background()
	q := benchQueue(b)
	payload := make([]byte, 512)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := q.Produce(ctx, payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkProduceBatch(b *testing.B) {
	ctx := context.Background()
	q := benchQueue(b)
	batch := make([][]byte, 64)
	for i := range batch {
		batch[i] = make([]byte, 256)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := q.ProduceBatch(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConsumeSequential(b *testing.B) {
	ctx := context.Background()
	const n = 50_000
	q := benchQueueFilled(b, n)

	b.ReportAllocs()
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
	const n = 50_000
	q := benchQueueFilled(b, n)

	// Pre-calculate lookup array to eliminate RNG overhead from timer
	offsets := make([]uint64, 10_000)
	for i := range offsets {
		offsets[i] = uint64(rand.IntN(n))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		offset := offsets[i%len(offsets)]
		if _, err := q.Read(ctx, offset); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConsumeParallel(b *testing.B) {
	ctx := context.Background()
	const n = 50_000
	q := benchQueueFilled(b, n)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			offset := uint64(i % n)
			i++
			if _, err := q.Read(ctx, offset); err != nil {
				b.Fatal(err)
			}
		}
	})
}
