package logqueue_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/enginetest"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShutdownWakesTail parks a Tail and asserts closing the storage yields ErrQueueClosed once and stops.
func TestShutdownWakesTail(t *testing.T) {
	storage := enginetest.NewMemStorage("tail-shutdown", 1<<20, 1<<20)
	lq := logqueue.New("tail-shutdown", storage)

	errCh := make(chan error, 1)
	go func() {
		for _, err := range lq.Tail(context.Background(), 0) {
			if err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()

	time.Sleep(100 * time.Millisecond)
	storage.Close()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, engine.ErrQueueClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("parked Tail did not stop on storage close")
	}
}

// newMemLogQueue builds a LogQueue over the in-memory enginetest storage so
// tail tests can run inside a testing/synctest bubble with a virtual clock and
// no real file I/O.
func newMemLogQueue(t *testing.T, name string) logqueue.Log {
	t.Helper()
	storage := enginetest.NewMemStorage(name, 1<<20, 1<<20)
	return logqueue.New(name, storage)
}

func TestLogQueueTailContinuous(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lq := newMemLogQueue(t, "tail-test")

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		received := make(chan string, 10)
		go func() {
			for msg, err := range lq.Tail(ctx, 0) {
				if err != nil {
					return
				}
				received <- string(msg.Payload)
			}
		}()

		const totalMsgs = 5
		for i := range totalMsgs {
			synctest.Sleep(20 * time.Millisecond)
			_, err := lq.Append(ctx, fmt.Appendf(nil, "msg-%d", i))
			require.NoError(t, err)
		}

		for i := range totalMsgs {
			select {
			case val := <-received:
				assert.Equal(t, fmt.Sprintf("msg-%d", i), val)
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("timed out waiting for tail message %d", i)
			}
		}
	})
}

func TestStreamReturnsImmediately(t *testing.T) {
	lq := newMemLogQueue(t, "stream-immediate")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	received := 0
	for msg, err := range lq.Stream(ctx, 0) {
		_ = msg
		_ = err
		received++
	}
	assert.Equal(t, 0, received)
}

func TestTailBatchDeliversAll(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lq := newMemLogQueue(t, "tail-batch")

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		const total = 100
		for i := range total {
			_, err := lq.Append(ctx, fmt.Appendf(nil, "msg-%d", i))
			require.NoError(t, err)
		}

		received := make(chan int, total)
		go func() {
			count := 0
			for msg, err := range lq.Tail(ctx, 0) {
				if err != nil {
					return
				}
				_ = msg
				count++
				if count >= total {
					received <- count
					return
				}
			}
		}()

		select {
		case count := <-received:
			assert.Equal(t, total, count)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out")
		}
	})
}

func TestLogQueueTailBugProof(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lq := newMemLogQueue(t, "tail-proof")

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// 1. Produce 1 message BEFORE starting Tail
		_, err := lq.Append(ctx, []byte("msg-0"))
		require.NoError(t, err)

		received := make(chan string, 10)
		go func() {
			for msg, err := range lq.Tail(ctx, 0) {
				if err != nil {
					close(received)
					return
				}
				received <- string(msg.Payload)
			}
		}()

		// 2. Sleep briefly to let Tail consume msg-0 and hit the head of the log
		synctest.Sleep(100 * time.Millisecond)

		// 3. Produce msg-1 AFTER Tail has caught up
		_, err = lq.Append(ctx, []byte("msg-1"))
		require.NoError(t, err)

		assert.Equal(t, "msg-0", <-received, "First message should arrive")

		select {
		case val, ok := <-received:
			require.True(t, ok, "Channel was closed because Tail() died on ErrOffsetNotFound!")
			assert.Equal(t, "msg-1", val)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Tail() died after msg-0 and failed to tail msg-1!")
		}
	})
}

// TestStreamSkipsCompactedOffsets: Stream yields only live records.
func TestStreamSkipsCompactedOffsets(t *testing.T) {
	dir := t.TempDir()
	mgr, err := engine.OpenManager(dir, engine.Config{
		Durability:    spec.DurabilityAsync,
		Compaction:    spec.CompactionKeys,
		MaxSegment:    256,
		MaxSegmentAge: time.Hour,
		MaxPayload:    1024 * 1024,
		MaxCache:      32 << 20,
		MaxFlushBytes: 1 << 20,
		MaxFlushWait:  10 * time.Millisecond,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })

	storage, err := mgr.Create("stream-skip")
	require.NoError(t, err)
	lq := logqueue.New("stream-skip", storage)
	ctx := t.Context()

	// 100B payloads with a 256B segment cap: one record per segment, so the
	// three same-key versions force two rolls and two compactions.
	key := []byte("user-42")
	for i := range 3 {
		_, err := lq.Append(ctx, bytes.Repeat([]byte{byte('v' + i)}, 100), logqueue.WithKey(key))
		require.NoError(t, err)
	}

	var got []engine.Record
	for rec, err := range lq.Stream(ctx, 0) {
		require.NoError(t, err)
		got = append(got, rec)
	}
	require.Len(t, got, 1)
	assert.Equal(t, uint64(2), got[0].Offset)
	assert.Equal(t, key, got[0].Group)
}
