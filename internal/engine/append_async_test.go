package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dmunish/mikyu/internal/spec"
)

// openAsyncQueue opens a raw async-durability queue for append-path tests.
func openAsyncQueue(t *testing.T, name string, mutate func(*Config)) *Queue {
	t.Helper()
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	cfg := testConfig()
	cfg.Durability = spec.DurabilityAsync
	if mutate != nil {
		mutate(&cfg)
	}
	q, err := openQueue(dir, name, cfg, store, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	return q
}

// parkFlusher blocks the WAL flusher inside its first write until the returned
// release func runs; release also stops the flusher so no goroutine outlives
// the writerHook cleanup.
func parkFlusher(t *testing.T, q *Queue) (release func()) {
	t.Helper()
	entered := make(chan struct{})
	left := make(chan struct{})
	var enteredOnce, leftOnce sync.Once
	releaseCh := make(chan struct{})
	writerHook = func() {
		enteredOnce.Do(func() { close(entered) })
		<-releaseCh
		leftOnce.Do(func() { close(left) })
	}
	t.Cleanup(func() { writerHook = nil })

	_, err := q.Append(t.Context(), []byte("park"))
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("flusher never reached its first write")
	}
	return func() {
		close(releaseCh)
		<-left
		require.NoError(t, q.w.stop())
	}
}

// fillAsyncChan fills the WAL async channel with filler slices that carry no reservations.
func fillAsyncChan(t *testing.T, q *Queue) {
	t.Helper()
	base := q.nextOffset + 10_000 // out-of-stream offsets: valid index slots, never published
	for i := range cap(q.w.asyncChan) {
		q.w.asyncChan <- []asyncEntry{{
			seg:    q.active,
			rec:    make([]byte, 8),
			pos:    1<<20 + int64(i)*8,
			recLen: 8,
			offset: base + uint64(i),
		}}
	}
}

// TestAppendBatchDoesNotHoldLockOnChannelBlock pins the phase contract: a
// producer blocked on a full async channel must not hold q.mu.
func TestAppendBatchDoesNotHoldLockOnChannelBlock(t *testing.T) {
	q := openAsyncQueue(t, "blocked", nil)
	release := parkFlusher(t, q)
	defer release()
	fillAsyncChan(t, q)

	posBefore := q.active.pos
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := q.AppendBatch(ctx, [][]byte{[]byte("late")})
		done <- err
	}()

	require.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.reservedPos > 0
	}, 2*time.Second, time.Millisecond, "appender never reserved its batch")

	acquired := make(chan struct{})
	go func() {
		q.mu.Lock()
		close(acquired)
		q.mu.Unlock()
	}()
	select {
	case <-acquired:
	case <-time.After(10 * time.Millisecond):
		t.Fatal("q.mu is held while a producer is blocked on the async channel")
	}

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.reservedPos == 0 && len(q.reservations) == 0
	}, 2*time.Second, time.Millisecond)
	q.mu.Lock()
	require.Equal(t, posBefore, q.active.pos, "an aborted reservation must not advance the segment")
	q.mu.Unlock()
}

// TestAppendBatchRollbackOnCtxCancel pins the rollback: canceling mid-push
// consumes no segment space and leaves the active segment untouched.
func TestAppendBatchRollbackOnCtxCancel(t *testing.T) {
	q := openAsyncQueue(t, "rollback", nil)
	release := parkFlusher(t, q)
	defer release()
	fillAsyncChan(t, q)

	posBefore := q.active.pos
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := q.AppendBatch(ctx, [][]byte{[]byte("a"), []byte("b"), []byte("c")})
		done <- err
	}()
	require.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.reservedPos > 0
	}, 2*time.Second, time.Millisecond)
	cancel()

	require.ErrorIs(t, <-done, context.Canceled)
	require.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.reservedPos == 0 && len(q.reservations) == 0 && q.active.pos == posBefore
	}, 2*time.Second, time.Millisecond)
}

// TestAppendBatchPreservesOffsetsUnderContention runs concurrent producers
// through a one-slot async channel and requires the offset stream to stay
// gap-free and fully readable.
func TestAppendBatchPreservesOffsetsUnderContention(t *testing.T) {
	q := openAsyncQueue(t, "contended", func(c *Config) { c.MaxFlushBytes = 4096 })

	const goroutines = 8
	const batches = 100
	const batchLen = 4
	const total = goroutines * batches * batchLen

	var mu sync.Mutex
	got := make(map[uint64]bool)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for b := range batches {
				payloads := make([][]byte, batchLen)
				for i := range batchLen {
					payloads[i] = []byte{byte(g), byte(b), byte(i)}
				}
				offs, err := q.AppendBatch(t.Context(), payloads)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				for _, o := range offs {
					if got[o] {
						mu.Unlock()
						t.Errorf("offset %d returned twice", o)
						return
					}
					got[o] = true
				}
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()

	q.mu.Lock()
	published, reserved, pending := q.publishedOffset, q.reservedPos, len(q.reservations)
	q.mu.Unlock()
	require.Zero(t, pending, "every reservation must resolve")
	require.Zero(t, reserved)
	require.Equal(t, uint64(total), published, "published offset must cover every record")
	require.Len(t, got, total)
	for o := uint64(0); o < total; o++ {
		require.True(t, got[o], "offset %d missing from the stream", o)
	}

	for o := uint64(0); o < total; o++ {
		_, err := q.Read(t.Context(), o)
		require.NoError(t, err, "offset %d must be readable", o)
	}
}
