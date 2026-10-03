package engine

import (
	"encoding/binary"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testNow(off uint64) time.Time { return time.Unix(0, int64(off)+1) }

// TestRingCacheConcurrentReaderWriter races one writer against eight readers
// and requires every cache hit to carry an internally consistent record.
func TestRingCacheConcurrentReaderWriter(t *testing.T) {
	r := newRingCache(32 << 20)
	const ops = 1_000_000
	const workers = 8

	var written atomic.Uint64
	stop := make(chan struct{})
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for off := uint64(0); off < ops; off++ {
			select {
			case <-stop:
				return
			default:
			}
			payload := make([]byte, 16)
			binary.BigEndian.PutUint64(payload, off)
			r.Put(Record{Offset: off, Timestamp: testNow(off), Payload: payload})
			written.Store(off + 1)
		}
	}()

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 42))
			for range ops / workers {
				off := rng.Uint64N(written.Load() + 1)
				if off == 0 {
					continue
				}
				if msg, ok := r.Get(off - 1); ok {
					require.Equal(t, off-1, msg.Offset, "torn read: offset mismatch")
					require.Len(t, msg.Payload, 16)
					require.Equal(t, off-1, binary.BigEndian.Uint64(msg.Payload), "torn read: payload mismatch")
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	writerWG.Wait()
}

// TestRingCacheGetUnderEviction races readers against a writer and an
// evictor, requiring Get to return either a consistent record or a clean miss.
func TestRingCacheGetUnderEviction(t *testing.T) {
	r := newRingCache(64 << 10) // tiny budget so Put constantly evicts
	const count = 200_000

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for off := uint64(0); off < count; off++ {
			payload := make([]byte, 16)
			binary.BigEndian.PutUint64(payload, off)
			r.Put(Record{Offset: off, Timestamp: testNow(off), Payload: payload})
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for off := uint64(0); off < count; off++ {
			r.EvictBefore(off)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewPCG(1, 2))
		for range count / 4 {
			off := rng.Uint64N(count)
			if msg, ok := r.Get(off); ok {
				require.Equal(t, off, msg.Offset, "torn read: offset mismatch")
				require.Equal(t, off, binary.BigEndian.Uint64(msg.Payload), "torn read: payload mismatch")
			}
		}
	}()
	wg.Wait()
}
