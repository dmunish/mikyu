package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRemoteStreamDrains(t *testing.T) {
	c := newRemoteClient(t)
	lq, err := c.LogQueue("q")
	require.NoError(t, err)
	require.NoError(t, c.CreateQueue(t.Context(), "q"))

	for i := range 5 {
		_, err := lq.Append(t.Context(), []byte("msg"))
		require.NoError(t, err)
		_ = i
	}

	count := 0
	for msg, err := range lq.Stream(t.Context(), 0) {
		if err != nil {
			t.Fatal(err)
		}
		_ = msg
		count++
		if count >= 5 {
			break
		}
	}
}

func TestRemoteTailWakesOnAppend(t *testing.T) {
	c := newRemoteClient(t)
	lq, err := c.LogQueue("q")
	require.NoError(t, err)
	require.NoError(t, c.CreateQueue(t.Context(), "q"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan int)
	go func() {
		count := 0
		for _, err := range lq.Tail(ctx, 0) {
			if err != nil {
				return
			}
			count++
			if count >= 1 {
				done <- count
				return
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)
	_, err = lq.Append(ctx, []byte("wake"))
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Tail did not wake on append")
	}
}

func TestRemoteTailCancel(t *testing.T) {
	c := newRemoteClient(t)
	lq, err := c.LogQueue("q")
	require.NoError(t, err)
	require.NoError(t, c.CreateQueue(t.Context(), "q"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for msg, err := range lq.Tail(ctx, 0) {
		_ = msg
		_ = err
		break
	}
}
