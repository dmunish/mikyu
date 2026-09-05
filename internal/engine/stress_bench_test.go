//go:build bench

package engine

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// Helper to create a queue for benchmarking.
func newBenchQueue(b *testing.B, cfg Config) (*Queue, *OffsetStore) {
	b.Helper()
	dir := b.TempDir()
	store, err := openOffsetStore(dir)
	if err != nil {
		b.Fatal(err)
	}
	q, err := openQueue(dir, "bench", cfg, store)
	if err != nil {
		b.Fatal(err)
	}
	return q, store
}

// BenchmarkProduceOnly measures concurrent produce throughput.
func BenchmarkProduceOnly(b *testing.B) {
	for _, msgSize := range []int{64, 1024, 64 * 1024} {
		b.Run(fmt.Sprintf("size=%d", msgSize), func(b *testing.B) {
			payload := make([]byte, msgSize)
			for i := range payload {
				payload[i] = byte(i)
			}
			q, store := newBenchQueue(b, Config{
				MaxMessageBytes: int64(msgSize * 2),
				BatchSize:       256,
				MaxInterval:     time.Millisecond,
			})
			defer store.Close()
			defer q.Close()

			ctx := context.Background()
			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := q.Append(ctx, payload); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// BenchmarkReadOnly measures concurrent read throughput from offset 0.
func BenchmarkReadOnly(b *testing.B) {
	for _, msgSize := range []int{64, 1024, 64 * 1024} {
		b.Run(fmt.Sprintf("size=%d", msgSize), func(b *testing.B) {
			payload := make([]byte, msgSize)
			q, store := newBenchQueue(b, Config{
				MaxMessageBytes: int64(msgSize * 2),
				BatchSize:       256,
				MaxInterval:     time.Millisecond,
			})
			defer store.Close()
			defer q.Close()

			// Pre-fill enough messages for reading.
			const prefill = 10000
			ctx := context.Background()
			for i := 0; i < prefill; i++ {
				if _, err := q.Append(ctx, payload); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			b.ReportAllocs()
			// Each goroutine reads sequentially from 0, but we must avoid
			// reading beyond the prefilled range. We'll reset the offset
			// counter randomly (but safely) or just read a fixed offset.
			// For read-only, we'll read a random offset within [0, prefill)
			// to simulate a typical read workload.
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					off := uint64(rng.Intn(prefill))
					_, err := q.Read(ctx, off)
					if err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// BenchmarkMixedWorkload simulates a mix of produce and read operations.
func BenchmarkMixedWorkload(b *testing.B) {
	for _, workload := range []struct {
		name     string
		produceP int // percentage of produce operations (0-100)
	}{
		{name: "50produce_50read", produceP: 50},
		{name: "25produce_75read", produceP: 25},
		{name: "75produce_25read", produceP: 75},
	} {
		b.Run(workload.name, func(b *testing.B) {
			q, store := newBenchQueue(b, Config{
				MaxMessageBytes: 1024 * 1024,
				BatchSize:       256,
				MaxInterval:     time.Millisecond,
			})
			defer store.Close()
			defer q.Close()

			ctx := context.Background()
			payload := []byte("some payload")

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					if rng.Intn(100) < workload.produceP {
						if _, err := q.Append(ctx, payload); err != nil {
							b.Error(err)
							return
						}
					} else {
						// Read a random offset; misses are acceptable.
						off := uint64(rng.Intn(10000))
						_, err := q.Read(ctx, off)
						if err != nil && err != ErrOffsetNotFound {
							b.Error(err)
							return
						}
					}
				}
			})
		})
	}
}

// BenchmarkBatchOperations measures batch produce and read throughput.
func BenchmarkBatchOperations(b *testing.B) {
	for _, batchSize := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			q, store := newBenchQueue(b, Config{
				MaxMessageBytes: 1024 * 1024,
				BatchSize:       256,
				MaxInterval:     time.Millisecond,
			})
			defer store.Close()
			defer q.Close()

			ctx := context.Background()
			payloads := make([][]byte, batchSize)
			for i := range payloads {
				payloads[i] = fmt.Appendf(nil, "msg-%d", i)
			}

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					offsets, err := q.AppendBatch(ctx, payloads)
					if err != nil {
						b.Error(err)
						return
					}
					// Read back the batch (optional, to simulate full pipeline)
					if len(offsets) > 0 {
						_, err = q.ReadBatch(ctx, offsets[0], len(offsets))
						if err != nil && err != ErrOffsetNotFound {
							b.Error(err)
							return
						}
					}
				}
			})
		})
	}
}

// BenchmarkHighConcurrencyStress simulates a burst of many concurrent workers
// doing a mix of operations (produce, read, batch).
func BenchmarkHighConcurrencyStress(b *testing.B) {
	q, store := newBenchQueue(b, Config{
		MaxMessageBytes: 64 * 1024,
		BatchSize:       256,
		MaxInterval:     time.Millisecond,
	})
	defer store.Close()
	defer q.Close()

	ctx := context.Background()
	payload := []byte("stress payload")

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	concurrency := 32
	totalOps := b.N
	opsPerG := totalOps / concurrency

	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < opsPerG; i++ {
				switch rng.Intn(3) {
				case 0:
					if _, err := q.Append(ctx, payload); err != nil {
						b.Error(err)
						return
					}
				case 1:
					if _, err := q.Read(ctx, uint64(rng.Intn(5000))); err != nil && err != ErrOffsetNotFound {
						b.Error(err)
						return
					}
				case 2:
					batch := make([][]byte, rng.Intn(10)+1)
					for j := range batch {
						batch[j] = payload
					}
					if _, err := q.AppendBatch(ctx, batch); err != nil {
						b.Error(err)
						return
					}
				}
			}
		}(int64(g))
	}
	wg.Wait()
}
