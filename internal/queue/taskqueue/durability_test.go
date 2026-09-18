package taskqueue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestShutdownWakesParkedDequeue parks a Dequeue and asserts Close wakes it with ErrTaskClosed.
func TestShutdownWakesParkedDequeue(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)

	errCh := make(chan error, 1)
	go func() {
		_, err := q.Dequeue(context.Background())
		errCh <- err
	}()

	time.Sleep(100 * time.Millisecond)
	require.NoError(t, q.Close())

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, ErrTaskClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("parked Dequeue did not wake on Close")
	}
}