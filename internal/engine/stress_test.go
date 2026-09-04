package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentProduceReadRace is a black-box stress test for the data race
// between Roll() (which writes mmapData) and Read() (which reads mmapData after
// releasing the lock). Run with `go test -race` to trigger the race detector.
func TestConcurrentProduceReadRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	q, err := openQueue(dir, "race", Config{MaxSegmentBytes: 256, BatchSize: 10}, store)
	require.NoError(t, err)
	defer q.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		producers = 5
		readers   = 5
		perOp     = 100
	)
	totalMsgs := uint64(producers * perOp)

	var wg sync.WaitGroup

	// Producers: write new records to trigger segment rolls.
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perOp; i++ {
				payload := fmt.Appendf(nil, "producer-%d-msg-%d", id, i)
				_, err := q.Produce(ctx, payload)
				assert.NoError(t, err)
			}
		}(p)
	}

	// Readers: read records until all produced messages are verified.
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			offset := uint64(0)
			for offset < totalMsgs {
				msg, err := q.Read(ctx, offset)
				if errors.Is(err, ErrOffsetNotFound) {
					time.Sleep(1 * time.Millisecond)
					continue
				}
				if err != nil {
					assert.NoError(t, err)
					return
				}
				_ = msg.Payload
				offset++
			}
		}()
	}

	wg.Wait()
}

// TestQueuePermanentErrorOnFlushFailure
// Simulates a real disk-full scenario by injecting a permanent write error into the WAL.
func TestQueuePermanentErrorOnFlushFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	q, err := openQueue(dir, "perm-error", Config{BatchSize: 1, MaxInterval: time.Millisecond}, store)
	require.NoError(t, err)
	defer q.Close()

	ctx := context.Background()

	// Inject a permanent error into the WAL (simulating a full disk)
	q.mu.Lock()
	q.w.err = errors.New("simulated disk full")
	q.mu.Unlock()

	// First Produce fails.
	_, err = q.Produce(ctx, []byte("msg1"))
	assert.Error(t, err)

	// Even after "freeing" the disk by setting the error to nil,
	// the queue SHOULD still be broken (this is the bug).
	// In the real world, a disk full is transient; the queue should recover once space is free.
	// Current implementation does NOT recover because it holds onto w.err forever.
	q.mu.Lock()
	q.w.err = nil // try to recover
	q.mu.Unlock()

	// This SHOULD succeed but currently fails silently (wait returns the old error).
	_, err = q.Produce(ctx, []byte("msg2"))
	assert.NoError(t, err, "queue should recover after disk space becomes available")
}
