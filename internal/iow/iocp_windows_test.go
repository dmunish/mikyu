//go:build windows

package iow

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

// newTestBatch builds a one-operation batch that already holds its slot, so the
// reaper's settle path has a token to release.
func newTestBatch() (*iocpBatch, *iocpWriter, chan error) {
	w := &iocpWriter{order: newOrder(), slots: make(chan struct{}, 1)}
	w.slots <- struct{}{}
	done := make(chan error, 1)
	b := &iocpBatch{writer: w, seq: w.order.reserve(), done: func(err error) { done <- err }, writes: 1}
	return b, w, done
}

// The kernel dequeues a failed operation as a packet whose overlapped is valid and
// whose error is reported by GetQueuedCompletionStatus itself. Skipping that packet
// leaks the operation and strands the batch behind a done that never fires.
func TestIOCPDispatchSettlesFailedPacket(t *testing.T) {
	p := &iocpPort{inflight: make(map[*iocpOp]struct{})}
	b, w, done := newTestBatch()
	op := &iocpOp{batch: b, length: 16}
	op.overlapped.Internal = uintptr(windows.STATUS_DISK_FULL)
	p.track(op)

	require.True(t, p.dispatch(windows.ERROR_WRITE_FAULT, 0, &op.overlapped))

	require.Equal(t, windows.STATUS_DISK_FULL.Errno(), <-done)
	require.Empty(t, p.inflight, "the completed operation must be unrooted")
	require.Empty(t, w.slots, "the batch's slot must be released")
	require.Error(t, w.currentError())
}

func TestIOCPDispatchRejectsShortAsyncWrite(t *testing.T) {
	p := &iocpPort{inflight: make(map[*iocpOp]struct{})}
	b, _, done := newTestBatch()
	op := &iocpOp{batch: b, length: 16}
	p.track(op)

	require.True(t, p.dispatch(nil, 8, &op.overlapped))

	require.ErrorIs(t, <-done, io.ErrShortWrite)
}

// A synchronously failed write already charges its operation, so the kernel's
// later failure packet must not drive the count below zero.
func TestIOCPDispatchIgnoresPacketAfterBatchFinished(t *testing.T) {
	p := &iocpPort{inflight: make(map[*iocpOp]struct{})}
	b, w, done := newTestBatch()
	op := &iocpOp{batch: b, length: 8}
	op.overlapped.Internal = uintptr(windows.STATUS_UNSUCCESSFUL)
	p.track(op)

	require.True(t, p.dispatch(windows.ERROR_WRITE_FAULT, 0, &op.overlapped))
	require.Error(t, <-done)

	b.opDone(nil)
	b.mu.Lock()
	defer b.mu.Unlock()
	require.Zero(t, b.writes)
	require.Equal(t, windows.STATUS_UNSUCCESSFUL.Errno(), b.err)
	require.Empty(t, p.inflight)
	require.Empty(t, w.slots)
}

func TestIOCPDispatchWakePacketHonoursStop(t *testing.T) {
	live := &iocpPort{stop: make(chan struct{})}
	require.True(t, live.dispatch(nil, 0, nil))

	done := &iocpPort{stop: make(chan struct{})}
	close(done.stop)
	require.False(t, done.dispatch(nil, 0, nil))

	abandoned := &iocpPort{}
	require.False(t, abandoned.dispatch(windows.ERROR_ABANDONED_WAIT_0, 0, nil))
	require.True(t, abandoned.dispatch(errors.New("transient"), 0, nil))
}

// close posts a wake packet with a nil overlapped; the reaper must observe the
// stop signal instead of parking in GetQueuedCompletionStatus forever.
func TestIOCPPortCloseStopsTheReaper(t *testing.T) {
	p := newPort()
	if p == nil {
		t.Skip("host has no usable completion port")
	}
	closed := make(chan struct{})
	go func() {
		p.close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("iocpPort.close did not stop its reaper")
	}
	p.close() // idempotent
}