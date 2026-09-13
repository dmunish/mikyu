package logqueue_test

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dmunish/mikyu/internal/enginetest"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMemLogQueue builds a LogQueue over the in-memory enginetest storage so
// tail tests can run inside a testing/synctest bubble with a virtual clock and
// no real file I/O.
func newMemLogQueue(t *testing.T, name string) *logqueue.LogQueue {
	t.Helper()
	storage := enginetest.NewMemStorage(name, 1<<20, 1<<20)
	return logqueue.NewLogQueue(name, storage)
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
