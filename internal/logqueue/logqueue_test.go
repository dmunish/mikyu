package logqueue_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/logqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogQueueTailContinuous(t *testing.T) {
	dir := t.TempDir()

	mgr, err := engine.OpenManager(dir, engine.Config{})
	require.NoError(t, err)
	defer mgr.Close()

	storage, err := mgr.CreateQueue("tail-test")
	require.NoError(t, err)

	lq := logqueue.NewLogQueue("tail-test", storage)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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
	for i := 0; i < totalMsgs; i++ {
		time.Sleep(20 * time.Millisecond)
		_, err := lq.Produce(ctx, fmt.Appendf(nil, "msg-%d", i))
		require.NoError(t, err)
	}

	for i := 0; i < totalMsgs; i++ {
		select {
		case val := <-received:
			assert.Equal(t, fmt.Sprintf("msg-%d", i), val)
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for tail message %d", i)
		}
	}
}

func TestLogQueueTailBugProof(t *testing.T) {
	dir := t.TempDir()
	mgr, err := engine.OpenManager(dir, engine.Config{})
	require.NoError(t, err)
	defer mgr.Close()

	storage, err := mgr.CreateQueue("tail-proof")
	require.NoError(t, err)

	lq := logqueue.NewLogQueue("tail-proof", storage)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Produce 1 message BEFORE starting Tail
	_, err = lq.Produce(ctx, []byte("msg-0"))
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
	time.Sleep(100 * time.Millisecond)

	// 3. Produce msg-1 AFTER Tail has caught up
	_, err = lq.Produce(ctx, []byte("msg-1"))
	require.NoError(t, err)

	assert.Equal(t, "msg-0", <-received, "First message should arrive")

	select {
	case val, ok := <-received:
		require.True(t, ok, "Channel was closed because Tail() died on ErrOffsetNotFound!")
		assert.Equal(t, "msg-1", val)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Tail() died after msg-0 and failed to tail msg-1!")
	}
}
